from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

try:
    import snowflake.connector
except Exception:
    snowflake = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OFFLINE_DATA_PATH = PROJECT_ROOT / "raw_data.csv"
DEFAULT_ROW_LIMIT = 20
SCRAPER_SCRIPT_PATH = PROJECT_ROOT / "scraper" / "scrape.py"


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str

    def is_complete(self) -> bool:
        required = [
            self.account,
            self.user,
            self.password,
            self.warehouse,
            self.database,
            self.schema,
        ]
        return all(bool(v and v.strip()) for v in required)


def get_default_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        database=os.getenv("SNOWFLAKE_DATABASE", "STOCK_PIPELINE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        role=os.getenv("SNOWFLAKE_ROLE", ""),
    )


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for col in ["category", "location", "symbol", "entity_name", "source_url"]:
        if col not in normalized.columns:
            normalized[col] = "Unknown" if col in {"category", "location"} else ""
    for col in ["price", "market_cap", "volume"]:
        if col not in normalized.columns:
            normalized[col] = pd.NA
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    if "observation_date" not in normalized.columns:
        if "scraped_at" in normalized.columns:
            normalized["observation_date"] = pd.to_datetime(
                normalized["scraped_at"], errors="coerce", utc=True
            ).dt.date
        else:
            normalized["observation_date"] = pd.NaT
    if "scraped_at" in normalized.columns:
        normalized["scraped_at"] = pd.to_datetime(
            normalized["scraped_at"], errors="coerce", utc=True
        )
    normalized["category"] = normalized["category"].fillna("Unknown")
    normalized["location"] = normalized["location"].fillna("Unknown")
    normalized["market_cap_category"] = pd.cut(
        normalized["market_cap"],
        bins=[-float("inf"), 2_000_000_000, 10_000_000_000, float("inf")],
        labels=["Small", "Mid", "Large"],
    ).astype("object")
    normalized["market_cap_category"] = normalized["market_cap_category"].fillna("Unknown")
    return normalized


def ensure_market_cap_category(df: pd.DataFrame) -> pd.DataFrame:
    ensured = df.copy()
    if "market_cap" not in ensured.columns:
        ensured["market_cap"] = pd.NA
    if "market_cap_category" not in ensured.columns:
        ensured["market_cap"] = pd.to_numeric(ensured["market_cap"], errors="coerce")
        ensured["market_cap_category"] = pd.cut(
            ensured["market_cap"],
            bins=[-float("inf"), 2_000_000_000, 10_000_000_000, float("inf")],
            labels=["Small", "Mid", "Large"],
        ).astype("object")
        ensured["market_cap_category"] = ensured["market_cap_category"].fillna("Unknown")
    return ensured


@st.cache_data(show_spinner=False)
def load_offline_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "scraped_at" not in df.columns:
        df["scraped_at"] = pd.NaT
    return _normalize_columns(df)


@st.cache_data(show_spinner=False)
def load_snowflake_data(config_dict: dict[str, str]) -> pd.DataFrame:
    if snowflake is None:
        raise RuntimeError("Snowflake connector not installed in environment.")

    query = """
    SELECT
      d.category,
      d.location,
      d.symbol,
      d.entity_name,
      f.price,
      f.market_cap,
      f.volume,
      f.observation_date,
      f.scraped_at,
      f.source_url
    FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
    JOIN STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY AS d
      ON f.entity_sk = d.entity_sk
    """

    conn_kwargs: dict[str, Any] = {
        "account": config_dict["account"],
        "user": config_dict["user"],
        "password": config_dict["password"],
        "warehouse": config_dict["warehouse"],
        "database": config_dict["database"],
        "schema": config_dict["schema"],
    }
    role = config_dict.get("role", "")
    if role:
        conn_kwargs["role"] = role

    conn = snowflake.connector.connect(**conn_kwargs)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return _normalize_columns(df)


@st.cache_data(show_spinner=False)
def get_filter_options(df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    prepared = ensure_market_cap_category(df)
    categories = sorted(v for v in prepared["category"].dropna().astype(str).unique() if v)
    locations = sorted(v for v in prepared["location"].dropna().astype(str).unique() if v)
    market_cap_categories = sorted(
        v for v in prepared["market_cap_category"].dropna().astype(str).unique() if v
    )
    return categories, locations, market_cap_categories


def apply_filters(
    df: pd.DataFrame,
    category: str,
    location: str,
    market_cap_category: str,
    metric_name: str,
    min_threshold: float,
    row_limit: int | None,
) -> pd.DataFrame:
    filtered = ensure_market_cap_category(df)
    if category != "All":
        filtered = filtered[filtered["category"] == category]
    if location != "All":
        filtered = filtered[filtered["location"] == location]
    if market_cap_category != "All":
        filtered = filtered[filtered["market_cap_category"] == market_cap_category]
    filtered = filtered[filtered[metric_name].fillna(0) >= min_threshold]
    filtered = filtered.sort_values(by=metric_name, ascending=False)
    if row_limit is None:
        return filtered
    return filtered.head(row_limit)


def compute_liquidity_shocks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "volume" not in df.columns:
        return pd.DataFrame(columns=["symbol", "entity_name", "volume", "volume_z_score"])

    work = df.copy()
    work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
    mean_volume = work["volume"].mean()
    std_volume = work["volume"].std()
    if pd.isna(std_volume) or std_volume == 0:
        return pd.DataFrame(columns=["symbol", "entity_name", "volume", "volume_z_score"])

    work["volume_z_score"] = (work["volume"] - mean_volume) / std_volume
    shocks = work[work["volume_z_score"] > 2].sort_values("volume_z_score", ascending=False)
    return shocks[["symbol", "entity_name", "volume", "volume_z_score"]]


def build_summary_text(results: pd.DataFrame) -> str:
    if results.empty:
        return "No rows matched the selected filters."
    avg_price = results["price"].dropna().mean()
    avg_volume = results["volume"].dropna().mean()
    top_symbol = str(results.iloc[0]["symbol"]) if "symbol" in results.columns else "N/A"
    return (
        f"Top result is {top_symbol}. "
        f"Average price across displayed rows is {avg_price:.2f}. "
        f"Average volume across displayed rows is {avg_volume:,.0f}."
    )


def summarize_results_with_llm(api_key: str, results: pd.DataFrame) -> str:
    # Demo-only placeholder: replace with provider SDK/API call in production.
    _ = api_key
    return build_summary_text(results)


def run_scraper_and_rebuild() -> tuple[bool, str]:
    if not SCRAPER_SCRIPT_PATH.exists():
        return False, f"Scraper script not found: {SCRAPER_SCRIPT_PATH}"
    try:
        completed = subprocess.run(
            [sys.executable, str(SCRAPER_SCRIPT_PATH)],
            cwd=str(PROJECT_ROOT),
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except Exception as exc:
        return False, f"Failed to execute scraper: {exc}"

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    details = "\n".join(part for part in [stdout, stderr] if part)
    if completed.returncode != 0:
        return False, details or "Scraper failed with a non-zero exit code."
    return True, details or "Scraper completed and raw files were rebuilt."


def render_connection_section() -> tuple[str, SnowflakeConfig]:
    st.sidebar.header("Data Source")
    mode = st.sidebar.radio("Mode", ["Auto", "Offline", "Snowflake"], index=0)
    defaults = get_default_config()
    with st.sidebar.expander("Snowflake Settings", expanded=False):
        account = st.text_input("Account", value=defaults.account)
        user = st.text_input("User", value=defaults.user)
        password = st.text_input("Password", value=defaults.password, type="password")
        warehouse = st.text_input("Warehouse", value=defaults.warehouse)
        database = st.text_input("Database", value=defaults.database)
        schema = st.text_input("Schema", value=defaults.schema)
        role = st.text_input("Role (optional)", value=defaults.role)
    return mode, SnowflakeConfig(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )


def get_dataset(mode: str, sf_config: SnowflakeConfig) -> tuple[pd.DataFrame, str]:
    if mode == "Offline":
        return load_offline_data(str(OFFLINE_DATA_PATH)), "offline"

    if mode in {"Auto", "Snowflake"} and sf_config.is_complete():
        try:
            return load_snowflake_data(sf_config.__dict__), "snowflake"
        except Exception as exc:
            if mode == "Snowflake":
                raise RuntimeError(f"Snowflake mode failed: {exc}") from exc
            st.warning(
                "Snowflake connection failed. Falling back to offline snapshot data."
            )
            return load_offline_data(str(OFFLINE_DATA_PATH)), "offline"

    if mode == "Snowflake":
        raise RuntimeError("Snowflake mode selected but required credentials are missing.")

    return load_offline_data(str(OFFLINE_DATA_PATH)), "offline"


def main() -> None:
    st.set_page_config(
        page_title="Market Activity & Risk Intelligence Dashboard – Most Actives",
        layout="wide",
    )
    st.title("Market Activity & Risk Intelligence Dashboard – Most Actives")
    st.caption("Run market-data filters, refresh pipelines, and summarize result sets.")
    st.divider()

    top_col1, top_col2 = st.columns(2, gap="large")
    with top_col1:
        st.subheader("Section 1: Local Raw Data Refresh")
        st.caption(
            "Re-run the scraper from the app to rebuild local `raw_data.csv` and `raw_data.json`."
        )
        if st.button("Run Scraper and Rebuild Raw Files"):
            with st.spinner("Running scraper..."):
                ok, message = run_scraper_and_rebuild()
            if ok:
                st.cache_data.clear()
                st.session_state["results_df"] = pd.DataFrame()
                st.success("Scraper completed. Local raw files were refreshed.")
                if message:
                    st.code(message)
                st.rerun()
            else:
                st.error("Scraper run failed. Check details below.")
                if message:
                    st.code(message)

    with top_col2:
        st.subheader("Section 2: Data Source Mode")
        st.caption("Choose Offline, Auto, or Snowflake mode from the sidebar.")
        mode, sf_config = render_connection_section()

        try:
            data, active_mode = get_dataset(mode, sf_config)
        except Exception as exc:
            st.error(str(exc))
            st.stop()

        if active_mode == "offline":
            st.info(
                "Offline mode active: using local snapshot data. "
                "Results may not reflect latest warehouse state."
            )
        else:
            st.success("Connected to Snowflake analytics tables.")
            if st.button("Update Snowflake Data"):
                st.cache_data.clear()
                st.session_state["results_df"] = pd.DataFrame()
                st.success("Refreshing Snowflake dataset...")
                st.rerun()

    st.divider()

    st.subheader("Section 3: Query Workspace")
    st.caption("Set filters, run query, review top results, and export/chart in one place.")
    categories, locations, market_cap_categories = get_filter_options(data)
    metric_name = st.selectbox("Metric", ["market_cap", "volume", "price"], index=0)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        category = st.selectbox("Category / Sector / Type", ["All", *categories], index=0)
    with col2:
        location = st.selectbox("Location / Country", ["All", *locations], index=0)
    with col3:
        market_cap_category = st.selectbox(
            "Market Cap Category",
            ["All", *market_cap_categories],
            index=0,
        )
    with col4:
        min_threshold = st.number_input(
            f"Minimum {metric_name}",
            min_value=0.0,
            value=0.0,
            step=1.0,
        )
    with col5:
        row_limit_option = st.selectbox(
            "Result row limit",
            ["5", "10", "20", "50", "100", "200", "All"],
            index=2,
        )

    selected_row_limit: int | None = None
    if row_limit_option != "All":
        selected_row_limit = int(row_limit_option)

    run_query = st.button("Run Query", type="primary")
    if "results_df" not in st.session_state:
        st.session_state["results_df"] = pd.DataFrame()
    if "results_seeded" not in st.session_state:
        st.session_state["results_df"] = apply_filters(
            data,
            category="All",
            location="All",
            market_cap_category="All",
            metric_name="market_cap",
            min_threshold=0.0,
            row_limit=DEFAULT_ROW_LIMIT,
        )
        st.session_state["results_seeded"] = True
    if run_query:
        st.session_state["results_df"] = apply_filters(
            data,
            category,
            location,
            market_cap_category,
            metric_name,
            min_threshold,
            selected_row_limit,
        )

    results = st.session_state["results_df"]
    insight_source = results if not results.empty else data
    total_market_volume = (
        pd.to_numeric(insight_source["volume"], errors="coerce").fillna(0).sum()
    )
    top5_volume = (
        insight_source.sort_values("volume", ascending=False)[["symbol", "entity_name", "volume"]]
        .head(5)
        .reset_index(drop=True)
    )
    liquidity_shocks = compute_liquidity_shocks(insight_source)
    top_volume_symbol = top5_volume.iloc[0]["symbol"] if not top5_volume.empty else "N/A"
    liquidity_shock_count = len(liquidity_shocks)

    with st.container(border=True):
        st.markdown("#### Stakeholder Insights")
        st.caption("Computed from the current filtered result set.")
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

        with kpi_col1:
            st.metric("Total Market Volume", f"{total_market_volume:,.0f}")
        with kpi_col2:
            st.metric("Liquidity Shock Count", f"{liquidity_shock_count}")
        with kpi_col3:
            st.metric("Top Volume Symbol", str(top_volume_symbol))

        insight_tab1, insight_tab2 = st.tabs(["Top 5 by Volume", "Liquidity Shocks"])
        with insight_tab1:
            st.dataframe(top5_volume, use_container_width=True, hide_index=True)
        with insight_tab2:
            if liquidity_shocks.empty:
                st.info("No liquidity shocks detected.")
            else:
                st.dataframe(
                    liquidity_shocks,
                    use_container_width=True,
                    hide_index=True,
                )

    st.markdown("#### Query Results")
    st.dataframe(results, use_container_width=True)
    st.caption(
        "Note: CSV can be downloaded from the table menu in the top-right corner."
    )

    if not results.empty:
        st.markdown("#### Visualization")
        chart_type = st.selectbox("Chart type", ["Bar", "Line"], index=0)
        if chart_type == "Line":
            chart = px.line(
                results,
                x="symbol",
                y=metric_name,
                markers=True,
                hover_data=["entity_name", "category", "location"],
                title=f"Top results by {metric_name} (Line)",
            )
        else:
            chart = px.bar(
                results,
                x="symbol",
                y=metric_name,
                hover_data=["entity_name", "category", "location"],
                title=f"Top results by {metric_name} (Bar)",
            )
        st.plotly_chart(chart, use_container_width=True)
    else:
        st.warning("No results yet. Click 'Run Query' to execute with current filters.")

    st.subheader("Section 6: LLM Summary (Demo)")
    st.warning(
        "Demo only: entering API keys directly in the UI is not secure and "
        "must not be used in production."
    )
    llm_api_key = st.text_input("LLM API Key (Demo)", type="password", key="llm_api_key")
    if st.button("Summarize results with LLM"):
        if results.empty:
            st.warning("Run a query first to generate results for summarization.")
        elif not llm_api_key.strip():
            st.warning("Please enter an API key to run summary in demo mode.")
        else:
            summary = summarize_results_with_llm(llm_api_key, results)
            st.success("Summary generated.")
            st.write(summary)


if __name__ == "__main__":
    main()
