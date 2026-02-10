from __future__ import annotations

import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MOST_ACTIVES_PAGE_URL = (
    "https://finance.yahoo.com/research-hub/screener/most_actives?start=0&count=100"
)
YAHOO_SCREENER_API = (
    "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
)
TARGET_ROW_COUNT = 100
PAGE_SIZE = 25


def fetch_page(start: int, count: int, max_retries: int = 4) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {"scrIds": "most_actives", "count": count, "start": start}
    )
    url = f"{YAHOO_SCREENER_API}?{params}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": MOST_ACTIVES_PAGE_URL,
    }

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            result = payload.get("finance", {}).get("result", [])
            if not result:
                return []
            return result[0].get("quotes", [])
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt == max_retries - 1:
                break
            time.sleep(1.5 * (2**attempt))

    raise RuntimeError(
        f"Failed to fetch Yahoo most-actives page chunk (start={start}, count={count})"
    ) from last_error


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def normalize_row(raw_quote: dict[str, Any], scraped_at: str) -> dict[str, Any]:
    return {
        "symbol": raw_quote.get("symbol"),
        "entity_name": raw_quote.get("longName")
        or raw_quote.get("shortName")
        or raw_quote.get("symbol"),
        "category": raw_quote.get("typeDisp")
        or raw_quote.get("quoteType")
        or "Unknown",
        "location": raw_quote.get("region") or "Unknown",
        "exchange": raw_quote.get("fullExchangeName") or raw_quote.get("exchange"),
        "currency": raw_quote.get("currency"),
        "price": to_float(raw_quote.get("regularMarketPrice")),
        "market_cap": to_int(raw_quote.get("marketCap")),
        "volume": to_int(raw_quote.get("regularMarketVolume")),
        "avg_volume_3m": to_int(raw_quote.get("averageDailyVolume3Month")),
        "source_url": MOST_ACTIVES_PAGE_URL,
        "scraped_at": scraped_at,
    }


def collect_most_actives() -> list[dict[str, Any]]:
    scraped_at = datetime.now(UTC).isoformat()
    records: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()

    for start in range(0, TARGET_ROW_COUNT, PAGE_SIZE):
        page_quotes = fetch_page(start=start, count=PAGE_SIZE)
        if not page_quotes:
            continue
        for quote in page_quotes:
            symbol = quote.get("symbol")
            if not symbol or symbol in seen_symbols:
                continue
            records.append(normalize_row(quote, scraped_at=scraped_at))
            seen_symbols.add(symbol)
            if len(records) >= TARGET_ROW_COUNT:
                break
        if len(records) >= TARGET_ROW_COUNT:
            break

    return records[:TARGET_ROW_COUNT]


def validate_output(records: list[dict[str, Any]]) -> None:
    required_columns = {
        "entity_name",
        "category",
        "location",
        "price",
        "market_cap",
        "source_url",
        "scraped_at",
    }
    if len(records) != TARGET_ROW_COUNT:
        raise ValueError(
            f"Expected exactly {TARGET_ROW_COUNT} rows, got {len(records)} rows."
        )
    missing = required_columns - set(records[0].keys())
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    numeric_columns = ["price", "market_cap", "volume"]
    min_required_non_null = int(TARGET_ROW_COUNT * 0.9)
    for column in numeric_columns:
        non_null_count = sum(1 for row in records if row.get(column) is not None)
        if non_null_count < min_required_non_null:
            raise ValueError(
                f"Column '{column}' has insufficient numeric coverage: "
                f"{non_null_count}/{TARGET_ROW_COUNT}"
            )


def write_outputs(records: list[dict[str, Any]]) -> tuple[Path, Path]:
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "raw_data.csv"
    json_path = project_root / "raw_data.json"

    fieldnames = list(records[0].keys())
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(records, json_file, ensure_ascii=False, indent=2)

    return csv_path, json_path


def main() -> None:
    records = collect_most_actives()
    validate_output(records)
    csv_path, json_path = write_outputs(records)
    print(f"Scraped rows: {len(records)}")
    print(f"CSV output: {csv_path}")
    print(f"JSON output: {json_path}")


if __name__ == "__main__":
    main()
