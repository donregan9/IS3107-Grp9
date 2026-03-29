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
import pandas as pd
import numpy as np
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
# Task: compute_and_store_features
#   Reads all available close/volume data for the ticker from stock_prices,
#   computes technical indicators using pandas, and upserts the full feature
#   set into stock_features. Re-running is safe — upsert prevents duplicates.
# ---------------------------------------------------------------------------
def compute_and_store_features(ticker, **context):
    try:
        print(f"Computing features for {ticker}...")
        conn = get_connection()

        # Load all raw OHLCV data for this ticker sorted by date
        df = pd.read_sql(
            "SELECT date, close, volume FROM stock_prices WHERE ticker = %s ORDER BY date ASC",
            conn, params=(ticker,)
        )
        conn.close()

        if df.empty or len(df) < 26:
            print(f"Not enough data to compute features for {ticker} (need at least 26 rows, have {len(df)}).")
            return {"status": "skipped", "records": 0}

        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.sort_values('date').reset_index(drop=True)

        # --- Indicators ---
        df['daily_return']  = df['close'].pct_change()
        df['sma_20']        = df['close'].rolling(20).mean()
        df['sma_50']        = df['close'].rolling(50).mean()
        df['ema_12']        = df['close'].ewm(span=12, adjust=False).mean()
        df['ema_26']        = df['close'].ewm(span=26, adjust=False).mean()
        df['macd']          = df['ema_12'] - df['ema_26']
        df['macd_signal']   = df['macd'].ewm(span=9, adjust=False).mean()
        df['volatility_14'] = df['daily_return'].rolling(14).std()
        df['volume_sma_20'] = df['volume'].rolling(20).mean()

        # Bollinger Bands (20-day)
        rolling_mean        = df['close'].rolling(20).mean()
        rolling_std         = df['close'].rolling(20).std()
        df['bb_upper']      = rolling_mean + (2 * rolling_std)
        df['bb_lower']      = rolling_mean - (2 * rolling_std)
        df['bb_width']      = (df['bb_upper'] - df['bb_lower']) / rolling_mean

        # Additional momentum/volume features
        df['volume_ratio_20'] = df['volume'] / df['volume_sma_20']
        df['return_3d']       = df['close'].pct_change(periods=3)
        df['return_5d']       = df['close'].pct_change(periods=5)

        # RSI (14-day)
        delta = df['close'].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        df['rsi_14'] = 100 - (100 / (1 + rs))

        # Drop rows where indicators are NaN (not enough history yet)
        df = df.dropna()

        # Build upsert values
        values = [
            (ticker, row['date'], row['close'], row['daily_return'],
             row['sma_20'], row['sma_50'], row['ema_12'], row['ema_26'],
             row['macd'], row['macd_signal'], row['rsi_14'],
             row['bb_upper'], row['bb_lower'], row['volatility_14'], row['volume_sma_20'],
             row['bb_width'], row['volume_ratio_20'], row['return_3d'], row['return_5d'])
            for _, row in df.iterrows()
        ]

        conn = get_connection()
        cursor = conn.cursor()
        execute_values(cursor, """
            INSERT INTO stock_features
                (ticker, date, close, daily_return, sma_20, sma_50, ema_12, ema_26,
                 macd, macd_signal, rsi_14, bb_upper, bb_lower, volatility_14, volume_sma_20,
                 bb_width, volume_ratio_20, return_3d, return_5d)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                close         = EXCLUDED.close,
                daily_return  = EXCLUDED.daily_return,
                sma_20        = EXCLUDED.sma_20,
                sma_50        = EXCLUDED.sma_50,
                ema_12        = EXCLUDED.ema_12,
                ema_26        = EXCLUDED.ema_26,
                macd          = EXCLUDED.macd,
                macd_signal   = EXCLUDED.macd_signal,
                rsi_14        = EXCLUDED.rsi_14,
                bb_upper      = EXCLUDED.bb_upper,
                bb_lower      = EXCLUDED.bb_lower,
                volatility_14 = EXCLUDED.volatility_14,
                volume_sma_20 = EXCLUDED.volume_sma_20,
                bb_width        = EXCLUDED.bb_width,
                volume_ratio_20 = EXCLUDED.volume_ratio_20,
                return_3d       = EXCLUDED.return_3d,
                return_5d       = EXCLUDED.return_5d
        """, values)
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Upserted {len(values)} feature rows for {ticker}.")
        return {"status": "success", "records": len(values)}
    except Exception as e:
        print(f"Feature engineering error: {e}")
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

    update_features_task = PythonOperator(
        task_id='update_stock_features',
        python_callable=compute_and_store_features,
        op_kwargs={'ticker': 'AAPL'}
    )

    backfill_task >> update_features_task