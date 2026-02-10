from __future__ import annotations

import os
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
MAX_ROWS = 20


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
    return normalized


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
def get_filter_options(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    categories = sorted(v for v in df["category"].dropna().astype(str).unique() if v)
    locations = sorted(v for v in df["location"].dropna().astype(str).unique() if v)
    return categories, locations


def apply_filters(
    df: pd.DataFrame,
    category: str,
    location: str,
    metric_name: str,
    min_threshold: float,
) -> pd.DataFrame:
    filtered = df.copy()
    if category != "All":
        filtered = filtered[filtered["category"] == category]
    if location != "All":
        filtered = filtered[filtered["location"] == location]
    filtered = filtered[filtered[metric_name].fillna(0) >= min_threshold]
    filtered = filtered.sort_values(by=metric_name, ascending=False)
    return filtered.head(MAX_ROWS)


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
    st.set_page_config(page_title="Stock Analytics Trigger UI", layout="wide")
    st.title("Stock Analytics Trigger UI")
    st.caption("Filter transformed stock observations and run quick analytics.")

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

    categories, locations = get_filter_options(data)
    metric_name = st.selectbox("Metric", ["market_cap", "volume", "price"], index=0)
    col1, col2, col3 = st.columns(3)
    with col1:
        category = st.selectbox("Category / Sector / Type", ["All", *categories], index=0)
    with col2:
        location = st.selectbox("Location / Country", ["All", *locations], index=0)
    with col3:
        min_threshold = st.number_input(
            f"Minimum {metric_name}",
            min_value=0.0,
            value=0.0,
            step=1.0,
        )

    run_query = st.button("Run Query", type="primary")
    if "results_df" not in st.session_state:
        st.session_state["results_df"] = pd.DataFrame()
    if run_query:
        st.session_state["results_df"] = apply_filters(
            data, category, location, metric_name, min_threshold
        )

    results = st.session_state["results_df"]
    st.subheader("Top 20 Results")
    st.dataframe(results, use_container_width=True)

    if not results.empty:
        csv_bytes = results.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Export Results to CSV",
            data=csv_bytes,
            file_name="query_results_top20.csv",
            mime="text/csv",
        )
        chart = px.bar(
            results,
            x="symbol",
            y=metric_name,
            hover_data=["entity_name", "category", "location"],
            title=f"Top results by {metric_name}",
        )
        st.plotly_chart(chart, use_container_width=True)
    else:
        st.warning("No results yet. Click 'Run Query' to execute with current filters.")

    st.subheader("Summarize Results with LLM")
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
