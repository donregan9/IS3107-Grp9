"""
Run to fill predictions on missed days
"""

import sys
import os
import logging
import pickle

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make scripts/ importable (same pattern as lstm_dag.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv('DB_HOST', 'postgres')
DB_USER     = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME     = os.getenv('DB_NAME', 'airflow')
DB_PORT     = os.getenv('DB_PORT', '5432')

TICKER       = 'AAPL'
SEQUENCE_LEN = 30    # must match lstm_model.py
PREDICT_DAYS = 21    # ← change this to generate more/fewer predictions

FEATURE_COLS = [
    'close', 'daily_return', 'sma_20', 'sma_50',
    'ema_12', 'ema_26', 'macd', 'macd_signal',
    'rsi_14', 'bb_upper', 'bb_lower', 'volatility_14', 'volume_sma_20',
    'bb_width', 'volume_ratio_20', 'return_3d', 'return_5d'
]

# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------
def _get_conn():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )

# ---------------------------------------------------------------------------
# Model artefact paths  (mirrors lstm_model.py)
# ---------------------------------------------------------------------------
def _model_dir(ticker: str) -> str:
    airflow_home = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
    return os.path.join(airflow_home, 'models', ticker)

def _model_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'lstm_model.keras')

def _scaler_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'scaler.pkl')

def _version_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'model_version.txt')

def _current_version(ticker: str):
    p = _version_path(ticker)
    if os.path.exists(p):
        with open(p) as f:
            return f.read().strip()
    return None

# ---------------------------------------------------------------------------
# Helper: next trading day  (mirrors lstm_model.py)
# ---------------------------------------------------------------------------
def _next_trading_day(dt) -> date:
    if hasattr(dt, 'date'):
        dt = dt.date()
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:   # 5=Sat, 6=Sun
        next_day += timedelta(days=1)
    return next_day

# ---------------------------------------------------------------------------
# Task: Fill the missed Days
# ---------------------------------------------------------------------------
def backfill_recent_predictions(ticker: str, **context):
    from tensorflow.keras.models import load_model

    # 1. --- Guard: check model artefacts exist ---
    version = _current_version(ticker)
    if version is None:
        raise FileNotFoundError(f"No trained model found for {ticker}.")

    model = load_model(_model_path(ticker))
    with open(_scaler_path(ticker), 'rb') as f:
        scaler = pickle.load(f)

    # 2. --- Smart Gap Detection ---
    conn = _get_conn()
    
    # We find dates where:
    # A) The prediction is totally missing
    # B) The prediction exists but actual_close is NULL (meaning the day has now passed)
    missing_data_df = pd.read_sql("""
        SELECT f.date 
        FROM stock_features f
        LEFT JOIN model_predictions p ON f.ticker = p.ticker 
            AND p.predicted_date > f.date 
            AND p.predicted_date <= f.date + interval '4 days'
        WHERE f.ticker = %s 
          AND (p.predicted_date IS NULL OR p.actual_close IS NULL)
        ORDER BY f.date ASC
    """, conn, params=(ticker,))
    conn.close()

    if missing_data_df.empty:
        log.info("No missing predictions or actuals to fill. Everything is up to date.")
        return

    # 3. --- Data Preparation ---
    conn = _get_conn()
    df = pd.read_sql(
        f"SELECT date, {', '.join(FEATURE_COLS)} FROM stock_features WHERE ticker = %s ORDER BY date ASC",
        conn, params=(ticker,)
    )
    actuals_df = pd.read_sql(
        "SELECT DATE(date) AS date, close FROM stock_prices WHERE ticker = %s",
        conn, params=(ticker,)
    )
    conn.close()

    df['date'] = pd.to_datetime(df['date']).dt.date
    actuals = dict(zip(pd.to_datetime(actuals_df['date']).dt.date, actuals_df['close']))
    
    # Identify indices in the dataframe that match our "missing" dates
    missing_data_df['date'] = pd.to_datetime(missing_data_df['date']).dt.date
    to_predict_indices = df[df['date'].isin(missing_data_df['date'])].index

    # 4. --- Inference Loop ---
    scaled = scaler.transform(df[FEATURE_COLS].values)
    records = []

    for idx in to_predict_indices:
        if idx < SEQUENCE_LEN: continue 
            
        window_end_date = df['date'].iloc[idx]
        predicted_date = _next_trading_day(window_end_date)
        actual_close = actuals.get(predicted_date)

        # Build sequence and predict
        X = scaled[idx - SEQUENCE_LEN + 1 : idx + 1, :].reshape(1, SEQUENCE_LEN, len(FEATURE_COLS))
        raw_pred = model.predict(X, verbose=0)[0, 0]
        
        pad = np.zeros((1, len(FEATURE_COLS)))
        pad[0, 0] = raw_pred
        predicted_close = float(scaler.inverse_transform(pad)[0, 0])

        records.append((ticker, predicted_date, predicted_close, actual_close, version))

    # 5. --- Selective SQL Upsert ---
    if records:
        conn = _get_conn()
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO model_predictions
                (ticker, predicted_date, predicted_close, actual_close, model_version)
            VALUES %s
            ON CONFLICT (ticker, predicted_date) 
            DO UPDATE SET
                actual_close = EXCLUDED.actual_close
        """, records)

        conn.commit()
        cur.close()
        conn.close()
        log.info(f"Processed {len(records)} records. Filled in missing predictions and updated actual prices.")
    
    return {"status": "success", "backfilled": len(records)}

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='missed_predictions',
    description=f'Catch up with missed predictions',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['lstm', 'backfill', 'evaluation', 'recent'],
    default_args={},
) as dag:

    backfill_task = PythonOperator(
        task_id='backfill_recent_predictions',
        python_callable=backfill_recent_predictions,
        op_kwargs={
            'ticker':       TICKER,
        },
    )

    backfill_task