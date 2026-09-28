# Live Crypto Prices Pipeline (Airflow + Postgres + Docker)

An Apache Airflow pipeline that pulls live cryptocurrency prices from the CoinGecko API every 5 minutes, cleans them, and stores the history in PostgreSQL. Everything runs locally with Docker.

## What it does

The `crypto_prices_pipeline` DAG runs every 5 minutes with three tasks:

```
extract_data  ->  transform_data  ->  load_data
```

1. **Extract**: calls the CoinGecko `simple/price` endpoint for Bitcoin, Ethereum and Solana (USD and INR)
2. **Transform**: converts the nested API response into flat rows and turns the epoch timestamp into a proper UTC datetime
3. **Load**: creates the `crypto_prices` table if needed and appends new rows

## Project structure

```
.
├── docker-compose.yml
├── dags/
│   └── crypto_prices_dag.py
├── .gitignore
└── README.md
```

## Tech stack

- Apache Airflow 2.10 (LocalExecutor)
- PostgreSQL 16 (one for Airflow metadata, one for the price data)
- Python: `requests`, `PostgresHook`
- Docker Compose

## How to run

**Requirements:** Docker Desktop (with WSL 2 on Windows).

```bash
docker compose up -d
```

The first run downloads images and can take 5-10 minutes.

Open Airflow at http://localhost:8081 (username `admin`, password `admin`). The DAG is enabled by default and starts running on its own.

The Airflow connection `my_postgres` is created automatically through the `AIRFLOW_CONN_MY_POSTGRES` environment variable in `docker-compose.yml`, so nothing needs to be set up in the UI.

## Check the data

```bash
docker compose exec data-db psql -U myuser -d mydata -c "SELECT coin, price_usd, price_inr, price_updated_at FROM crypto_prices ORDER BY price_updated_at DESC LIMIT 10;"
```

You can also connect with DBeaver or pgAdmin: host `localhost`, port `5434`, user `myuser`, password `mypass`, database `mydata`.

## Table schema

| Column | Type | Meaning |
|---|---|---|
| `id` | SERIAL | Row number |
| `coin` | TEXT | Coin id (bitcoin, ethereum, solana) |
| `price_usd` | NUMERIC | Price in US dollars |
| `price_inr` | NUMERIC | Price in Indian rupees |
| `price_updated_at` | TIMESTAMPTZ | When CoinGecko last updated the price |
| `fetched_at` | TIMESTAMPTZ | When this pipeline stored the row |

## Stop

```bash
docker compose down        # stop, keep data
docker compose down -v     # stop and delete all data
```

## Design notes

- **No duplicate rows:** `UNIQUE (coin, price_updated_at)` with `ON CONFLICT DO NOTHING`. CoinGecko does not always update a price within 5 minutes, so the same data can come back, and this rule skips it silently.
- **History, not overwrite:** each run appends rows, so you can chart price changes over time.
- **Rate limits:** CoinGecko's free API is rate limited, so keep the schedule at 5 minutes or slower.
- **Credentials:** the passwords in `docker-compose.yml` are demo values for local use only. For anything real, use a `.env` file and never commit it.
- **Changing coins:** edit the `COINS` list at the top of `dags/crypto_prices_dag.py` using CoinGecko coin ids.

## Ideas for next steps

- Add a dashboard (Metabase or Streamlit) on top of `crypto_prices`
- Add data quality checks between transform and load
- Add failure alerts (email or Slack)
