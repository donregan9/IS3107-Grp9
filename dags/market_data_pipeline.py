# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import os

# ---------------------------------------------------------------------------
# Configuration (reads from environment variables, with safe defaults)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv('DB_HOST', 'postgres')
DB_USER     = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME     = os.getenv('DB_NAME', 'airflow')
DB_PORT     = os.getenv('DB_PORT', '5432')

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY', 'XHD8MQIGSGDCPGSY')

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def get_connection():
    """Return a new psycopg2 connection using the configured credentials."""
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
    """Upsert a list of (ticker, date, open, high, low, close, volume) tuples."""
    conn = get_connection()
    cursor = conn.cursor()
    execute_values(cursor, UPSERT_QUERY, values)
    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------------------------------
# Task 1 – create_table
#   Creates the stock_prices table (and index) if they don't already exist.
#   Runs every DAG execution but is a no-op after the first run.
# ---------------------------------------------------------------------------
def create_table():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id         SERIAL PRIMARY KEY,
                ticker     VARCHAR(10),
                date       TIMESTAMP,
                open       FLOAT,
                high       FLOAT,
                low        FLOAT,
                close      FLOAT,
                volume     BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, date)
            );
            CREATE INDEX IF NOT EXISTS idx_ticker_date ON stock_prices(ticker, date);

            -- Add updated_at if it doesn't exist (handles tables created before this column was added)
            ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

            -- Add unique constraint if it doesn't exist (needed for upsert)
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'stock_prices_ticker_date_key'
                ) THEN
                    ALTER TABLE stock_prices ADD CONSTRAINT stock_prices_ticker_date_key UNIQUE (ticker, date);
                END IF;
            END $$;

            CREATE TABLE IF NOT EXISTS model_predictions (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                predicted_date  DATE,
                predicted_close FLOAT,
                actual_close    FLOAT,        -- filled in the next day for comparison
                model_version   VARCHAR(50),
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, predicted_date)
            );
            CREATE INDEX IF NOT EXISTS idx_pred_ticker_date ON model_predictions(ticker, predicted_date);

            CREATE TABLE IF NOT EXISTS stock_features (
                id             SERIAL PRIMARY KEY,
                ticker         VARCHAR(10),
                date           DATE,
                close          FLOAT,
                daily_return   FLOAT,
                sma_20         FLOAT,
                sma_50         FLOAT,
                ema_12         FLOAT,
                ema_26         FLOAT,
                macd           FLOAT,
                macd_signal    FLOAT,
                rsi_14         FLOAT,
                bb_upper       FLOAT,
                bb_lower       FLOAT,
                volatility_14  FLOAT,
                volume_sma_20  FLOAT,
                UNIQUE(ticker, date)
            );
            CREATE INDEX IF NOT EXISTS idx_feat_ticker_date ON stock_features(ticker, date);

            CREATE TABLE IF NOT EXISTS model_metadata (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                model_version   VARCHAR(50) UNIQUE,
                trained_at      TIMESTAMP,
                training_start  DATE,         -- oldest date used for training
                training_end    DATE,         -- newest date used for training
                mae             FLOAT,        -- mean absolute error on validation set
                rmse            FLOAT,        -- root mean squared error on validation set
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table ready.")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

# ---------------------------------------------------------------------------
# Task 2 – compute_and_store_features  (DAILY)
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
             row['bb_upper'], row['bb_lower'], row['volatility_14'], row['volume_sma_20'])
            for _, row in df.iterrows()
        ]

        conn = get_connection()
        cursor = conn.cursor()
        execute_values(cursor, """
            INSERT INTO stock_features
                (ticker, date, close, daily_return, sma_20, sma_50, ema_12, ema_26,
                 macd, macd_signal, rsi_14, bb_upper, bb_lower, volatility_14, volume_sma_20)
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
                volume_sma_20 = EXCLUDED.volume_sma_20
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
# Task 3 – fetch_and_store_stock_data  (DAILY)
#   Fetches only yesterday's OHLCV row and upserts it.
#   Runs every day on the @daily schedule.
#   Upsert means it's safe if the same day is fetched more than once.
# ---------------------------------------------------------------------------
def fetch_and_store_stock_data(ticker, **context):
    try:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Fetching {ticker} for {date_str} from Alpha Vantage...")

        url = (
            f'https://www.alphavantage.co/query'
            f'?function=TIME_SERIES_DAILY'
            f'&symbol={ticker}'
            f'&apikey={ALPHA_VANTAGE_API_KEY}'
        )
        data = requests.get(url).json()
        time_series = data.get("Time Series (Daily)", {})

        if date_str not in time_series:
            print(f"No data for {ticker} on {date_str} (non-trading day?).")
            return {"status": "no_data", "records": 0}

        row = time_series[date_str]
        values = [(
            ticker, date_str,
            float(row['1. open']), float(row['2. high']),
            float(row['3. low']),  float(row['4. close']),
            int(row['5. volume'])
        )]

        upsert_values(values)
        print(f"Upserted 1 record for {ticker} on {date_str}.")
        return {"status": "success", "records": 1}
    except Exception as e:
        print(f"Daily fetch error: {e}")
        return {"status": "failed", "error": str(e)}

# ---------------------------------------------------------------------------
# DAG definition
#   Schedule: @daily (runs once per day)
#   Task order: create_table → fetch_daily_stock_data
#   For first-time historical data load, trigger the backfill_historical_data DAG manually.
# ---------------------------------------------------------------------------
with DAG(
    dag_id='market_momentum_extraction',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    description='Fetch daily AAPL stock data from Alpha Vantage and store in PostgreSQL'
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    daily_fetch_task = PythonOperator(
        task_id='fetch_daily_stock_data',
        python_callable=fetch_and_store_stock_data,
        op_kwargs={'ticker': 'AAPL'}
    )

    feature_engineering_task = PythonOperator(
        task_id='compute_features',
        python_callable=compute_and_store_features,
        op_kwargs={'ticker': 'AAPL'}
    )

    create_table_task >> daily_fetch_task >> feature_engineering_task
