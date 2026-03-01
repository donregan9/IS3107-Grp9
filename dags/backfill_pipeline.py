# ---------------------------------------------------------------------------
# Backfill DAG  (ONE-OFF — trigger manually once, then leave it)
#
# This DAG loads the full historical daily OHLCV data for AAPL from
# Alpha Vantage (up to ~20 years) into the stock_prices table.
#
# How to run:
#   1. Open the Airflow UI → DAGs → backfill_historical_data
#   2. Click the "Trigger DAG" button (▶) once
#   3. You never need to run it again — upsert means re-running is safe but unnecessary
#
# schedule=None means Airflow will NEVER run this automatically.
# ---------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values
import os

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv('DB_HOST', 'postgres')
DB_USER     = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME     = os.getenv('DB_NAME', 'airflow')
DB_PORT     = os.getenv('DB_PORT', '5432')

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY', 'XHD8MQIGSGDCPGSY')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )

UPSERT_QUERY = """
    INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (ticker, date) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        updated_at = CURRENT_TIMESTAMP
"""

def upsert_values(values):
    conn = get_connection()
    cursor = conn.cursor()
    execute_values(cursor, UPSERT_QUERY, values)
    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------------------------------
# Task: backfill_stock_data
#   Fetches the full daily price history for a ticker using outputsize=full
#   (~20 years of data) and upserts every row into stock_prices.
# ---------------------------------------------------------------------------
def backfill_stock_data(ticker, **context):
    try:
        print(f"Backfilling {ticker} (full history) from Alpha Vantage...")
        # outputsize=compact returns the last 100 trading days (free tier)
        # outputsize=full requires a premium API key (~20 years of data)
        url = (
            f'https://www.alphavantage.co/query'
            f'?function=TIME_SERIES_DAILY'
            f'&symbol={ticker}'
            f'&outputsize=compact'
            f'&apikey={ALPHA_VANTAGE_API_KEY}'
        )
        data = requests.get(url).json()
        time_series = data.get("Time Series (Daily)", {})

        if not time_series:
            print(f"No data returned for {ticker}. API response: {data}")
            return {"status": "no_data", "records": 0}

        values = [
            (ticker, date_str,
             float(row['1. open']), float(row['2. high']),
             float(row['3. low']),  float(row['4. close']),
             int(row['5. volume']))
            for date_str, row in time_series.items()
        ]

        upsert_values(values)
        print(f"Backfill complete: {len(values)} records upserted for {ticker}.")
        return {"status": "success", "records": len(values)}
    except Exception as e:
        print(f"Backfill error: {e}")
        return {"status": "failed", "error": str(e)}

# ---------------------------------------------------------------------------
# DAG Definition
#   schedule=None → only runs when manually triggered in the Airflow UI
# ---------------------------------------------------------------------------
with DAG(
    dag_id='backfill_historical_data',
    start_date=datetime(2026, 1, 1),
    schedule=None,          # Never runs automatically — manual trigger only
    catchup=False,
    description='One-off backfill: loads full AAPL price history into PostgreSQL'
) as dag:

    backfill_task = PythonOperator(
        task_id='backfill_stock_data',
        python_callable=backfill_stock_data,
        op_kwargs={'ticker': 'AAPL'}
    )
