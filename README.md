# Stock Snowflake dbt Mini Pipeline

Mini pipeline for stock analytics:
- scrape Yahoo Most Actives data
- load into Snowflake raw + clean layers
- transform with dbt into staging/dimension/fact models
- run stakeholder analytics SQL
- trigger filtered analytics through a Streamlit UI

## Dataset Source

- Yahoo Finance Most Actives:
  `https://finance.yahoo.com/research-hub/screener/most_actives?start=0&count=100`

## Project Structure

- `scraper/` - scraping script and scraper dependencies
- `sql/` - Snowflake DDL, load SQL, analytics SQL
- `dbt_project/` - dbt models and tests
- `app/` - Streamlit frontend trigger UI
- `analysis_answers.md` - stakeholder insights and automation ideas

## 1) Run the Scraper

From repository root:

```bash
python3 -m pip install -r scraper/requirements.txt
python3 scraper/scrape.py
```

Expected outputs in repo root:
- `raw_data.csv`
- `raw_data.json`

## 2) Create / Load Snowflake Tables

Use scripts in order:

1. `sql/01_create_tables.sql`  
   Creates:
   - `STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_RAW`
   - `STOCK_PIPELINE_DB.ANALYTICS.MOST_ACTIVES_CLEAN`

2. `sql/02_load_data.sql`  
   - defines file format + stage
   - loads CSV into raw table (`COPY INTO`)
   - merges typed data into clean table (`MERGE`)
   - includes strict quality checks

Typical load sequence in Snowflake worksheet:
- run `sql/01_create_tables.sql`
- upload `raw_data.csv` to the declared stage (see `PUT` example in script)
- run `sql/02_load_data.sql`

## 3) Run dbt Models and Tests

Inside `dbt_project/`:

```bash
dbt deps
dbt parse
dbt run
dbt test
```

Built models:
- `stg_most_actives`
- `dim_entity`
- `fct_observations`

## 4) Run Analytics Queries

Run:
- `sql/03_analytics_queries.sql`

Includes:
- top-10 ranking query
- trend query
- outlier detection
- geographic breakdown
- stakeholder risk-watch query

## 5) Run the Frontend App (Phase 5)

Install dependencies:

```bash
python3 -m pip install -r app/requirements.txt
```

Launch:

```bash
streamlit run app/streamlit_app.py
```

### App Features

- Dropdown filter: category/sector/type
- Dropdown filter: location/country
- Numeric threshold input for selected metric
- `Run Query` button (returns top 20 rows)
- CSV export button
- Bar chart for result set
- `Summarize results with LLM` button
- Caching for data load and filter options

### Offline vs Snowflake Mode

- **Offline mode** (default-safe): reads local `raw_data.csv` and still supports full UI interactions.
- **Snowflake mode**: reads from:
  - `STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS`
  - `STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY`

For Snowflake mode, set environment variables (or input via sidebar):

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE` (default `STOCK_PIPELINE_DB`)
- `SNOWFLAKE_SCHEMA` (default `ANALYTICS`)
- `SNOWFLAKE_ROLE` (optional)

## Demo API Key Safety Note

The app includes a demo API key input for LLM summarization.

- This is **for demo purposes only**.
- Entering API keys directly in UI is not production-safe.
- In production, use secure secret management (environment variables, Streamlit secrets, or a secrets manager).
