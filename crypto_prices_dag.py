"""
LIVE CRYPTO PRICES PIPELINE

Har 5 minute mein:
  1. extract_data   -> CoinGecko API se Bitcoin/Ethereum/Solana ka live price laao
  2. transform_data -> data ko saaf rows mein badlo (coin, USD price, INR price, time)
  3. load_data      -> Postgres ki crypto_prices table mein NAYI rows jodo

Pichle pipeline (users) mein purani row update hoti thi.
Yahan history chahiye, isliye har baar nayi row add hoti hai.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
import requests

# 👉 Yahan apne coins badal sakte ho (CoinGecko ke coin id likhne hain)
COINS = ["bitcoin", "ethereum", "solana"]


# ---------------------------------------------------------------------
# STEP 1: EXTRACT: API se live price laao
# ---------------------------------------------------------------------
def extract_data():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": "usd,inr",
        "include_last_updated_at": "true",
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"API se mila: {data}")
    return data


# ---------------------------------------------------------------------
# STEP 2: TRANSFORM: rows ki list banao
# ---------------------------------------------------------------------
def transform_data(ti):
    raw = ti.xcom_pull(task_ids="extract_data")

    rows = []
    for coin, values in raw.items():
        # API epoch seconds deti hai, usse normal datetime (UTC) mein badalte hain
        updated_at = datetime.fromtimestamp(values["last_updated_at"], tz=timezone.utc)
        rows.append({
            "coin": coin,
            "price_usd": values["usd"],
            "price_inr": values["inr"],
            "price_updated_at": updated_at.isoformat(),
        })

    return rows


# ---------------------------------------------------------------------
# STEP 3: LOAD: Postgres mein nayi rows jodo
# ---------------------------------------------------------------------
def load_data(ti):
    rows = ti.xcom_pull(task_ids="transform_data")
    hook = PostgresHook(postgres_conn_id="my_postgres")

    hook.run("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id               SERIAL PRIMARY KEY,
            coin             TEXT NOT NULL,
            price_usd        NUMERIC,
            price_inr        NUMERIC,
            price_updated_at TIMESTAMPTZ NOT NULL,
            fetched_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (coin, price_updated_at)
        );
    """)

    # UNIQUE + ON CONFLICT DO NOTHING: agar API ne wahi purana price diya
    # (jo pehle se table mein hai) to duplicate row nahi banegi.
    for r in rows:
        hook.run(
            """
            INSERT INTO crypto_prices (coin, price_usd, price_inr, price_updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (coin, price_updated_at) DO NOTHING;
            """,
            parameters=(r["coin"], r["price_usd"], r["price_inr"], r["price_updated_at"]),
        )

    print(f"{len(rows)} coins ki prices process ho gayi")


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
with DAG(
    dag_id="crypto_prices_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",      # har 5 minute mein
    catchup=False,
    tags=["crypto", "postgres", "realtime"],
) as dag:

    t_extract = PythonOperator(task_id="extract_data", python_callable=extract_data)
    t_transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t_load = PythonOperator(task_id="load_data", python_callable=load_data)

    t_extract >> t_transform >> t_load
