"""
Shared data validation helpers for Airflow DAGs that ingest from Alpha Vantage API.

This module provides reusable validation functions for:
1. API-level payload integrity checks
2. Row-level OHLCV domain validation
3. Quality check audit logging to data_quality_results table
"""

import json
import os
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values


# ============================================================================
# Database Connection
# ============================================================================

def get_connection():
    """Return a new psycopg2 connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        user=os.getenv('DB_USER', 'airflow'),
        password=os.getenv('DB_PASSWORD', 'airflow'),
        database=os.getenv('DB_NAME', 'airflow'),
        port=os.getenv('DB_PORT', '5432')
    )


# ============================================================================
# API-Level Validation
# ============================================================================

def fetch_alpha_vantage_daily(ticker, api_key, timeout_seconds=30, outputsize='compact'):
    """
    Fetch daily OHLCV payload and validate API-level response integrity.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL')
        api_key: Alpha Vantage API key
        timeout_seconds: HTTP request timeout in seconds
        outputsize: 'compact' (latest 100 trading days) or 'full' (20 years, premium key)
    
    Returns:
        dict: Time Series (Daily) mapping date strings to OHLCV data
    
    Raises:
        RuntimeError: On HTTP failure, non-JSON, API error, rate limit, or missing data
    """
    url = (
        f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'
        f'&symbol={ticker}&outputsize={outputsize}&apikey={api_key}'
    )
    
    try:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Alpha Vantage request failed for {ticker}: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError(f"Alpha Vantage returned non-JSON payload for {ticker}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Alpha Vantage response is not a JSON object")

    if data.get('Error Message'):
        raise RuntimeError(f"Alpha Vantage API error for {ticker}: {data['Error Message']}")

    if data.get('Note'):
        raise RuntimeError(f"Alpha Vantage rate limit or notice for {ticker}: {data['Note']}")

    time_series = data.get('Time Series (Daily)')
    if not isinstance(time_series, dict) or not time_series:
        raise RuntimeError(f"Alpha Vantage response missing Time Series (Daily) for {ticker}")

    return time_series


# ============================================================================
# Row-Level Validation
# ============================================================================

def validate_and_build_price_record(ticker, date_str, row):
    """
    Validate one OHLCV row and return a DB record tuple or a rejection reason.
    
    Args:
        ticker: Stock symbol
        date_str: Date string (YYYY-MM-DD)
        row: Dict with Alpha Vantage fields: '1. open', '2. high', '3. low', '4. close', '5. volume'
    
    Returns:
        tuple: (record_tuple, reason_str) where:
            - record_tuple is (ticker, date_str, open, high, low, close, volume) if valid
            - reason_str is a rejection code if invalid
    """
    required_fields = ('1. open', '2. high', '3. low', '4. close', '5. volume')

    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except Exception:
        return None, 'invalid_date'

    if not isinstance(row, dict):
        return None, 'invalid_row_type'

    missing_fields = [field for field in required_fields if field not in row]
    if missing_fields:
        return None, 'missing_fields'

    try:
        open_price = float(row['1. open'])
        high_price = float(row['2. high'])
        low_price = float(row['3. low'])
        close_price = float(row['4. close'])
        volume = int(float(row['5. volume']))
    except (TypeError, ValueError):
        return None, 'type_conversion_error'

    if min(open_price, high_price, low_price, close_price) <= 0:
        return None, 'non_positive_price'
    if volume < 0:
        return None, 'negative_volume'
    if high_price < low_price:
        return None, 'high_below_low'
    if not (low_price <= open_price <= high_price):
        return None, 'open_outside_high_low'
    if not (low_price <= close_price <= high_price):
        return None, 'close_outside_high_low'

    return (ticker, date_str, open_price, high_price, low_price, close_price, volume), None


def build_raw_price_observation(
    ticker,
    source_date_key,
    row,
    context=None,
    is_valid=None,
    reject_reason=None,
    source='alphavantage',
):
    """Build one raw-ingestion row tuple for stock_prices_raw."""
    run_id = None
    dag_id = None
    task_id = None

    if context:
        run_id = context.get('run_id')
        task_instance = context.get('task_instance')
        if task_instance:
            dag_id = task_instance.dag_id
            task_id = task_instance.task_id

    trade_date = None
    try:
        trade_date = datetime.strptime(source_date_key, '%Y-%m-%d').date()
    except Exception:
        trade_date = None

    def _safe_float(key):
        try:
            return float(row[key])
        except Exception:
            return None

    def _safe_int(key):
        try:
            return int(float(row[key]))
        except Exception:
            return None

    open_price = _safe_float('1. open') if isinstance(row, dict) else None
    high_price = _safe_float('2. high') if isinstance(row, dict) else None
    low_price = _safe_float('3. low') if isinstance(row, dict) else None
    close_price = _safe_float('4. close') if isinstance(row, dict) else None
    volume = _safe_int('5. volume') if isinstance(row, dict) else None

    return (
        ticker,
        source_date_key,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        json.dumps(row if isinstance(row, dict) else {'value': row}),
        is_valid,
        reject_reason,
        source,
        dag_id,
        task_id,
        run_id,
    )


def insert_raw_price_rows(raw_rows):
    """Bulk insert raw-ingestion rows into stock_prices_raw."""
    if not raw_rows:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    execute_values(
        cursor,
        """
        INSERT INTO stock_prices_raw
            (ticker, source_date_key, trade_date, open, high, low, close, volume,
             raw_payload, is_valid, reject_reason, source, dag_id, task_id, run_id)
        VALUES %s
        ON CONFLICT (ticker, source_date_key, run_id) DO UPDATE SET
            trade_date = EXCLUDED.trade_date,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            raw_payload = EXCLUDED.raw_payload,
            is_valid = EXCLUDED.is_valid,
            reject_reason = EXCLUDED.reject_reason,
            source = EXCLUDED.source,
            dag_id = EXCLUDED.dag_id,
            task_id = EXCLUDED.task_id,
            ingested_at = CURRENT_TIMESTAMP
        """,
        raw_rows,
    )
    conn.commit()
    cursor.close()
    conn.close()
    return len(raw_rows)


def ensure_raw_price_table_exists():
    """Create stock_prices_raw table if it does not exist yet."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_prices_raw (
            id              SERIAL PRIMARY KEY,
            ticker          VARCHAR(10) NOT NULL,
            source_date_key VARCHAR(20) NOT NULL,
            trade_date      DATE,
            open            FLOAT,
            high            FLOAT,
            low             FLOAT,
            close           FLOAT,
            volume          BIGINT,
            raw_payload     JSONB NOT NULL,
            is_valid        BOOLEAN,
            reject_reason   VARCHAR(100),
            source          VARCHAR(50) DEFAULT 'alphavantage',
            dag_id          VARCHAR(255),
            task_id         VARCHAR(255),
            run_id          VARCHAR(255),
            ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, source_date_key, run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_raw_ticker_date ON stock_prices_raw(ticker, trade_date);
        CREATE INDEX IF NOT EXISTS idx_raw_run_id ON stock_prices_raw(run_id);
        """
    )
    conn.commit()
    cursor.close()
    conn.close()


# ============================================================================
# Quality Audit Logging
# ============================================================================

def log_data_quality_check(
    check_name,
    status,
    context=None,
    ticker=None,
    observed_value=None,
    threshold=None,
    failed_count=0,
    details=None,
):
    """
    Persist one data-quality check result for observability and audit.
    
    Args:
        check_name: Name of the check (e.g., 'api_payload_valid', 'row_validation')
        status: 'pass' or 'fail'
        context: Airflow task context dict (optional, contains run_id and task info)
        ticker: Stock symbol (optional)
        observed_value: Numeric value observed in the check
        threshold: Numeric threshold for comparison
        failed_count: Number of items that failed
        details: Dict or string with additional details
    
    Returns:
        None (logs to database; prints warning if insert fails)
    """
    run_id = None
    dag_id = None
    task_id = None

    if context:
        run_id = context.get('run_id')
        task_instance = context.get('task_instance')
        if task_instance:
            dag_id = task_instance.dag_id
            task_id = task_instance.task_id

    details_text = details if isinstance(details, str) else json.dumps(details or {})

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO data_quality_results
                (dag_id, task_id, run_id, ticker, check_name, status,
                 observed_value, threshold, failed_count, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                dag_id,
                task_id,
                run_id,
                ticker,
                check_name,
                status,
                observed_value,
                threshold,
                failed_count,
                details_text,
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as exc:
        print(f"Warning: failed to write data quality check '{check_name}': {exc}")
