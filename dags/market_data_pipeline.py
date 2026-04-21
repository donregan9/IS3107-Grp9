# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

# Import shared validation helpers from scripts/data_validation.py
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
from data_validation import (
    fetch_alpha_vantage_daily as _fetch_av_daily_impl,
    validate_and_build_price_record,
    log_data_quality_check
)

# ---------------------------------------------------------------------------
# Configuration (reads from environment variables, with safe defaults)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv('DB_HOST', 'postgres')
DB_USER     = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME     = os.getenv('DB_NAME', 'airflow')
DB_PORT     = os.getenv('DB_PORT', '5432')

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY', 'XHD8MQIGSGDCPGSY')
FEATURES_READY_DATASET = Dataset('dataset://stock_features/aapl/ready')
API_TIMEOUT_SECONDS = 30
MAX_REJECT_RATIO = 0.20
MAX_API_STALENESS_DAYS = 5
MIN_INCREMENTAL_COMPLETENESS = 0.90
MAX_SOURCE_NULL_RATIO = 0.01

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def get_connection():
    """Return a new psycopg2 connection using the configured credentials."""
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )

# Wrapper to pass module-level API key and timeout to shared module function
def _fetch_alpha_vantage_daily(ticker, outputsize='compact'):
    """Fetch daily OHLCV payload with module-level API key and timeout."""
    return _fetch_av_daily_impl(ticker, ALPHA_VANTAGE_API_KEY, API_TIMEOUT_SECONDS, outputsize)

# Re-export validation helpers with underscore prefix for consistency
def _validate_and_build_price_record(ticker, date_str, row):
    """Validate one OHLCV row and return a DB record tuple or a rejection reason."""
    return validate_and_build_price_record(ticker, date_str, row)

def _log_data_quality_check(
    check_name, status, context=None, ticker=None, observed_value=None,
    threshold=None, failed_count=0, details=None
):
    """Persist one data-quality check result for observability and audit."""
    return log_data_quality_check(
        check_name, status, context, ticker, observed_value, threshold, failed_count, details
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
                bb_width        FLOAT,
                volume_ratio_20 FLOAT,
                return_3d       FLOAT,
                return_5d       FLOAT,
                UNIQUE(ticker, date)
            );
            CREATE INDEX IF NOT EXISTS idx_feat_ticker_date ON stock_features(ticker, date);

            -- Backward-compatible migrations for older stock_features schemas
            ALTER TABLE stock_features ADD COLUMN IF NOT EXISTS bb_width FLOAT;
            ALTER TABLE stock_features ADD COLUMN IF NOT EXISTS volume_ratio_20 FLOAT;
            ALTER TABLE stock_features ADD COLUMN IF NOT EXISTS return_3d FLOAT;
            ALTER TABLE stock_features ADD COLUMN IF NOT EXISTS return_5d FLOAT;

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

            CREATE TABLE IF NOT EXISTS data_quality_results (
                id             SERIAL PRIMARY KEY,
                dag_id         VARCHAR(255),
                task_id        VARCHAR(255),
                run_id         VARCHAR(255),
                ticker         VARCHAR(10),
                check_name     VARCHAR(100) NOT NULL,
                status         VARCHAR(20) NOT NULL,
                observed_value FLOAT,
                threshold      FLOAT,
                failed_count   INTEGER DEFAULT 0,
                details        TEXT,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_dq_run ON data_quality_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_dq_check ON data_quality_results(check_name);
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

        source_null_rows = int(df[['close', 'volume']].isnull().any(axis=1).sum())
        source_null_ratio = source_null_rows / len(df)
        _log_data_quality_check(
            check_name='source_base_null_ratio',
            status='pass' if source_null_ratio <= MAX_SOURCE_NULL_RATIO else 'fail',
            context=context,
            ticker=ticker,
            observed_value=source_null_ratio,
            threshold=MAX_SOURCE_NULL_RATIO,
            failed_count=source_null_rows,
            details={'rows': len(df), 'null_rows': source_null_rows}
        )
        if source_null_ratio > MAX_SOURCE_NULL_RATIO:
            raise ValueError(
                f"Source null ratio too high for {ticker}: {source_null_ratio:.2%} "
                f"(threshold {MAX_SOURCE_NULL_RATIO:.0%})"
            )

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

        if df.empty:
            _log_data_quality_check(
                check_name='feature_rows_after_dropna',
                status='fail',
                context=context,
                ticker=ticker,
                observed_value=0.0,
                failed_count=1,
                details={'message': 'No feature rows remain after dropna'}
            )
            raise ValueError(f"No feature rows remain after dropna for {ticker}.")

        _log_data_quality_check(
            check_name='feature_rows_after_dropna',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=float(len(df)),
            details={'message': 'Feature rows available for upsert'}
        )

        feature_cols = [
            'close', 'daily_return', 'sma_20', 'sma_50', 'ema_12', 'ema_26',
            'macd', 'macd_signal', 'rsi_14', 'bb_upper', 'bb_lower',
            'volatility_14', 'volume_sma_20', 'bb_width', 'volume_ratio_20',
            'return_3d', 'return_5d'
        ]

        feature_null_cells = int(df[feature_cols].isnull().sum().sum())
        _log_data_quality_check(
            check_name='feature_null_cells',
            status='pass' if feature_null_cells == 0 else 'fail',
            context=context,
            ticker=ticker,
            observed_value=float(feature_null_cells),
            threshold=0.0,
            failed_count=feature_null_cells,
            details={'feature_columns': feature_cols}
        )
        if feature_null_cells > 0:
            raise ValueError(f"Feature dataframe has {feature_null_cells} null cells after dropna for {ticker}.")

        rsi_out_of_range = int(((df['rsi_14'] < 0) | (df['rsi_14'] > 100)).sum())
        _log_data_quality_check(
            check_name='rsi_range_check',
            status='pass' if rsi_out_of_range == 0 else 'fail',
            context=context,
            ticker=ticker,
            observed_value=float(rsi_out_of_range),
            threshold=0.0,
            failed_count=rsi_out_of_range,
            details={'expected_range': '[0, 100]'}
        )
        if rsi_out_of_range > 0:
            raise ValueError(f"RSI out-of-range rows detected for {ticker}: {rsi_out_of_range}")

        negative_volatility_rows = int((df['volatility_14'] < 0).sum())
        _log_data_quality_check(
            check_name='volatility_non_negative',
            status='pass' if negative_volatility_rows == 0 else 'fail',
            context=context,
            ticker=ticker,
            observed_value=float(negative_volatility_rows),
            threshold=0.0,
            failed_count=negative_volatility_rows,
            details={'column': 'volatility_14'}
        )
        if negative_volatility_rows > 0:
            raise ValueError(f"Negative volatility rows detected for {ticker}: {negative_volatility_rows}")

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
        _log_data_quality_check(
            check_name='feature_rows_upserted',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=float(len(values)),
            details={'message': 'Feature rows upserted successfully'}
        )
        return {"status": "success", "records": len(values)}
    except Exception as e:
        _log_data_quality_check(
            check_name='feature_engineering_exception',
            status='fail',
            context=context,
            ticker=ticker,
            failed_count=1,
            details={'error': str(e)}
        )
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

        time_series = _fetch_alpha_vantage_daily(ticker, outputsize='compact')
        _log_data_quality_check(
            check_name='api_payload_valid',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=float(len(time_series)),
            details={'message': 'Time Series payload retrieved successfully'}
        )

        if date_str not in time_series:
            print(f"No data for {ticker} on {date_str} (non-trading day?).")
            _log_data_quality_check(
                check_name='daily_row_present',
                status='pass',
                context=context,
                ticker=ticker,
                observed_value=0.0,
                details={'message': 'No row for target date (likely non-trading day)'}
            )
            return {"status": "no_data", "records": 0}

        row = time_series[date_str]
        record, reason = _validate_and_build_price_record(ticker, date_str, row)
        if reason:
            _log_data_quality_check(
                check_name='daily_row_validation',
                status='fail',
                context=context,
                ticker=ticker,
                observed_value=1.0,
                failed_count=1,
                details={'reason': reason, 'date': date_str}
            )
            raise ValueError(f"Rejected daily row for {ticker} on {date_str}: {reason}")

        _log_data_quality_check(
            check_name='daily_row_validation',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=1.0,
            details={'date': date_str}
        )

        values = [record]

        upsert_values(values)
        print(f"Upserted 1 record for {ticker} on {date_str}.")
        _log_data_quality_check(
            check_name='rows_upserted',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=1.0,
            details={'message': 'Daily row upserted successfully'}
        )
        return {"status": "success", "records": 1}
    except Exception as e:
        _log_data_quality_check(
            check_name='daily_ingestion_exception',
            status='fail',
            context=context,
            ticker=ticker,
            failed_count=1,
            details={'error': str(e)}
        )
        print(f"Daily fetch error: {e}")
        return {"status": "failed", "error": str(e)}
    
# ---------------------------------------------------------------------------
#   Fetch Data from when Docker is inavtive  (DAILY)
#   Runs every day on the @daily schedule.
# ---------------------------------------------------------------------------

def smart_fetch_and_store(ticker, **context):
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Find the last date we successfully stored
    cur.execute("SELECT MAX(date) FROM stock_prices WHERE ticker = %s", (ticker,))
    last_date_raw = cur.fetchone()[0]
    conn.close()
    
    # If table is empty, default to 99 days ago
    last_date = last_date_raw.date() if last_date_raw else (datetime.now() - timedelta(days=99)).date()
    
    print(f"Last data point in DB: {last_date}. Fetching updates...")

    time_series = _fetch_alpha_vantage_daily(ticker, ALPHA_VANTAGE_API_KEY, API_TIMEOUT_SECONDS, outputsize='compact')
    _log_data_quality_check(
        check_name='api_payload_valid',
        status='pass',
        context=context,
        ticker=ticker,
        observed_value=float(len(time_series)),
        details={'message': 'Time Series payload retrieved successfully'}
    )

    api_dates = []
    invalid_api_date_keys = 0
    for date_str in time_series.keys():
        try:
            api_dates.append(datetime.strptime(date_str, '%Y-%m-%d').date())
        except ValueError:
            invalid_api_date_keys += 1

    if invalid_api_date_keys:
        _log_data_quality_check(
            check_name='api_date_key_format',
            status='fail',
            context=context,
            ticker=ticker,
            observed_value=float(invalid_api_date_keys),
            threshold=0.0,
            failed_count=invalid_api_date_keys,
            details={'message': 'Invalid date keys detected in API payload'}
        )

    if api_dates:
        latest_api_date = max(api_dates)
        api_staleness_days = (datetime.now().date() - latest_api_date).days
        _log_data_quality_check(
            check_name='api_freshness',
            status='pass' if api_staleness_days <= MAX_API_STALENESS_DAYS else 'fail',
            context=context,
            ticker=ticker,
            observed_value=float(api_staleness_days),
            threshold=float(MAX_API_STALENESS_DAYS),
            failed_count=1 if api_staleness_days > MAX_API_STALENESS_DAYS else 0,
            details={'latest_api_date': latest_api_date.isoformat()}
        )
        if api_staleness_days > MAX_API_STALENESS_DAYS:
            raise ValueError(
                f"API data is stale for {ticker}: latest={latest_api_date}, "
                f"staleness={api_staleness_days} days."
            )

        if latest_api_date > last_date:
            expected_new_market_days = len(
                pd.bdate_range(start=last_date + timedelta(days=1), end=latest_api_date)
            )
            available_new_market_days = len({d for d in api_dates if last_date < d <= latest_api_date})
            completeness_ratio = (
                (available_new_market_days / expected_new_market_days)
                if expected_new_market_days > 0 else 1.0
            )
            _log_data_quality_check(
                check_name='incremental_completeness',
                status='pass' if completeness_ratio >= MIN_INCREMENTAL_COMPLETENESS else 'fail',
                context=context,
                ticker=ticker,
                observed_value=completeness_ratio,
                threshold=MIN_INCREMENTAL_COMPLETENESS,
                failed_count=max(expected_new_market_days - available_new_market_days, 0),
                details={
                    'expected_market_days': expected_new_market_days,
                    'available_market_days': available_new_market_days,
                    'window_start': (last_date + timedelta(days=1)).isoformat(),
                    'window_end': latest_api_date.isoformat()
                }
            )
            if completeness_ratio < MIN_INCREMENTAL_COMPLETENESS:
                raise ValueError(
                    f"Incremental completeness too low for {ticker}: {completeness_ratio:.1%} "
                    f"(threshold {MIN_INCREMENTAL_COMPLETENESS:.0%})."
                )

    new_records = []
    rejected_records = 0
    rejected_reasons = {}

    for date_str, row in time_series.items():
        try:
            curr_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            rejected_records += 1
            rejected_reasons['invalid_date'] = rejected_reasons.get('invalid_date', 0) + 1
            continue
        
        # ONLY grab data newer than what we have
        if curr_date > last_date:
            record, reason = _validate_and_build_price_record(ticker, date_str, row)
            if reason:
                rejected_records += 1
                rejected_reasons[reason] = rejected_reasons.get(reason, 0) + 1
                continue
            new_records.append(record)

    total_candidate_rows = len(new_records) + rejected_records

    if total_candidate_rows > 0:
        _log_data_quality_check(
            check_name='row_validation',
            status='pass' if rejected_records == 0 else 'fail',
            context=context,
            ticker=ticker,
            observed_value=float(total_candidate_rows),
            threshold=MAX_REJECT_RATIO,
            failed_count=rejected_records,
            details={
                'accepted_rows': len(new_records),
                'rejected_rows': rejected_records,
                'reasons': rejected_reasons
            }
        )

    if rejected_records:
        print(
            f"Validation rejected {rejected_records}/{total_candidate_rows} new rows for {ticker}. "
            f"Reasons: {rejected_reasons}"
        )

    if rejected_records and not new_records:
        _log_data_quality_check(
            check_name='all_candidate_rows_rejected',
            status='fail',
            context=context,
            ticker=ticker,
            observed_value=float(total_candidate_rows),
            failed_count=rejected_records,
            details={'reasons': rejected_reasons}
        )
        raise ValueError(
            f"All candidate rows failed validation for {ticker}. "
            f"Reasons: {rejected_reasons}"
        )

    if total_candidate_rows > 0:
        reject_ratio = rejected_records / total_candidate_rows
        if reject_ratio > MAX_REJECT_RATIO:
            _log_data_quality_check(
                check_name='reject_ratio_threshold',
                status='fail',
                context=context,
                ticker=ticker,
                observed_value=reject_ratio,
                threshold=MAX_REJECT_RATIO,
                failed_count=rejected_records,
                details={'reasons': rejected_reasons}
            )
            raise ValueError(
                f"Rejected {rejected_records}/{total_candidate_rows} rows for {ticker} "
                f"({reject_ratio:.1%}), above threshold {MAX_REJECT_RATIO:.0%}."
            )

    if new_records:
        upsert_values(new_records)
        _log_data_quality_check(
            check_name='rows_upserted',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=float(len(new_records)),
            details={'rejected_rows': rejected_records}
        )
        print(
            f"Successfully caught up! Added {len(new_records)} missing days "
            f"(rejected: {rejected_records})."
        )
    else:
        _log_data_quality_check(
            check_name='rows_upserted',
            status='pass',
            context=context,
            ticker=ticker,
            observed_value=0.0,
            details={'message': 'No new rows to upsert'}
        )
        print("Database is already up to date.")

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
        python_callable=smart_fetch_and_store,
        op_kwargs={'ticker': 'AAPL'}
    )

    feature_engineering_task = PythonOperator(
        task_id='compute_features',
        python_callable=compute_and_store_features,
        op_kwargs={'ticker': 'AAPL'},
        outlets=[FEATURES_READY_DATASET]
    )

    create_table_task >> daily_fetch_task >> feature_engineering_task