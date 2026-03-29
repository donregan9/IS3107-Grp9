# ---------------------------------------------------------------------------
# Recent Predictions Backfill DAG  (ONE-OFF — trigger manually)
#
# Generates exactly PREDICT_DAYS predictions ending at the latest available
# date in stock_features, and immediately compares against actual closes.
#
# Logic:
#   Given SEQUENCE_LEN=30 and PREDICT_DAYS=30, fetch the last 60 rows.
#   Row 1–30  → predict Row 31  (first prediction)
#   Row 2–31  → predict Row 32
#   ...
#   Row 30–59 → predict Row 60  (last prediction = PREDICT_DAYS-th)
#
#   Formula: fetch last (SEQUENCE_LEN + PREDICT_DAYS) rows from stock_features
#
# To change the number of prediction days, update PREDICT_DAYS below.
#
# Prerequisites:
#   1. backfill_historical_data DAG has been run       (stock_prices populated)
#   2. market_momentum_extraction DAG has been run     (stock_features populated)
#   3. lstm_weekly_training DAG has been run at least once (model artefacts exist)
#
# How to run:
#   Airflow UI → DAGs → backfill_predictions_recent → Trigger DAG (▶)
#   Safe to re-run — upsert prevents duplicates.
# ---------------------------------------------------------------------------

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
# Task: backfill_recent_predictions
# ---------------------------------------------------------------------------
def backfill_recent_predictions(ticker: str, predict_days: int, **context):
    """
    Generates exactly `predict_days` predictions ending at the latest
    available date in stock_features.

    Fetches the last (SEQUENCE_LEN + predict_days) rows — just enough to
    slide a SEQUENCE_LEN window predict_days times.

    Example (SEQUENCE_LEN=30, predict_days=30):
      Fetches 60 rows.
      Window rows 1–30  → predicts row 31
      Window rows 2–31  → predicts row 32
      ...
      Window rows 30–59 → predicts row 60  (30th prediction)
    """
    from tensorflow.keras.models import load_model

    # ------------------------------------------------------------------
    # Guard: check model artefacts exist
    # ------------------------------------------------------------------
    version = _current_version(ticker)
    if version is None:
        raise FileNotFoundError(
            f"No trained model found for {ticker}. "
            "Run the lstm_weekly_training DAG first."
        )

    model = load_model(_model_path(ticker))
    with open(_scaler_path(ticker), 'rb') as f:
        scaler = pickle.load(f)

    log.info("Loaded model version: %s", version)

    # ------------------------------------------------------------------
    # Fetch exactly (SEQUENCE_LEN + predict_days) rows from stock_features
    # ------------------------------------------------------------------
    rows_needed = SEQUENCE_LEN + predict_days
    conn = _get_conn()
    df = pd.read_sql(
        f"""
        SELECT date, {', '.join(FEATURE_COLS)}
        FROM stock_features
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT %s
        """,
        conn, params=(ticker, rows_needed)
    )

    # Load actual closes for immediate comparison
    actuals_df = pd.read_sql(
        "SELECT DATE(date) AS date, close FROM stock_prices WHERE ticker = %s",
        conn, params=(ticker,)
    )
    conn.close()

    # Sort ascending after DESC fetch
    df = df.sort_values('date').reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date']).dt.date

    actuals_df['date'] = pd.to_datetime(actuals_df['date']).dt.date
    actuals = dict(zip(actuals_df['date'], actuals_df['close']))

    if len(df) < rows_needed:
        raise ValueError(
            f"Need {rows_needed} rows in stock_features "
            f"(SEQUENCE_LEN={SEQUENCE_LEN} + predict_days={predict_days}), "
            f"but only {len(df)} available."
        )

    log.info(
        "Fetched %d rows (%s → %s) to generate %d predictions.",
        len(df), df['date'].iloc[0], df['date'].iloc[-1], predict_days
    )

    # ------------------------------------------------------------------
    # Scale the fetched window once
    # ------------------------------------------------------------------
    scaled = scaler.transform(df[FEATURE_COLS].values)

    # ------------------------------------------------------------------
    # Slide the window exactly predict_days times
    # ------------------------------------------------------------------
    records = []

    for i in range(SEQUENCE_LEN, SEQUENCE_LEN + predict_days):
        window_end_date = df['date'].iloc[i - 1]
        predicted_date  = _next_trading_day(window_end_date)

        # Build input sequence
        X = scaled[i - SEQUENCE_LEN:i, :].reshape(1, SEQUENCE_LEN, len(FEATURE_COLS))

        # Predict (scaled space)
        raw_pred = model.predict(X, verbose=0)[0, 0]

        # Inverse-transform back to price
        pad = np.zeros((1, len(FEATURE_COLS)))
        pad[0, 0] = raw_pred
        predicted_close = float(scaler.inverse_transform(pad)[0, 0])

        # Actual close for immediate comparison
        actual_close = actuals.get(predicted_date)

        records.append((
            ticker,
            predicted_date,
            predicted_close,
            actual_close,
            version
        ))

        log.info(
            "  [%d/%d] %s → predicted: $%.4f | actual: %s",
            i - SEQUENCE_LEN + 1, predict_days, predicted_date,
            predicted_close,
            f"${actual_close:.4f}" if actual_close else "N/A"
        )

    # ------------------------------------------------------------------
    # Upsert into backfill_model_predictions
    # ------------------------------------------------------------------
    conn = _get_conn()
    cur  = conn.cursor()
    execute_values(cur, """
        INSERT INTO model_predictions
            (ticker, predicted_date, predicted_close, actual_close, model_version)
        VALUES %s
        ON CONFLICT (ticker, predicted_date) DO UPDATE SET
            actual_close    = EXCLUDED.actual_close
    """, records)
    conn.commit()
    cur.close()
    conn.close()

    # ------------------------------------------------------------------
    # Summary stats
    # ------------------------------------------------------------------
    compared = [(p, a) for _, _, p, a, _ in records if a is not None]
    if compared:
        preds       = np.array([p for p, _ in compared])
        actuals_arr = np.array([a for _, a in compared])
        mae  = float(np.mean(np.abs(preds - actuals_arr)))
        rmse = float(np.sqrt(np.mean((preds - actuals_arr) ** 2)))
        log.info(
            "Backfill summary — %d predictions | %d comparable | MAE: $%.4f | RMSE: $%.4f",
            len(records), len(compared), mae, rmse
        )
    else:
        mae, rmse = None, None
        log.info("No comparable rows (predicted dates are all in the future).")

    result = {
        "status":        "success",
        "records":       len(records),
        "comparable":    len(compared),
        "mae_price":     mae,
        "rmse_price":    rmse,
        "model_version": version,
        "predict_days":  predict_days,
    }

    context['ti'].xcom_push(key='backfill_recent_result', value=result)
    return result


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='backfill_predictions',
    description=f'One-off: predict the last {PREDICT_DAYS} days and compare with actuals',
    start_date=datetime(2026, 1, 1),
    schedule=None,      # manual trigger only
    catchup=False,
    tags=['lstm', 'backfill', 'evaluation', 'recent'],
    default_args={
        'retries': 1,
    },
) as dag:

    backfill_task = PythonOperator(
        task_id='backfill_recent_predictions',
        python_callable=backfill_recent_predictions,
        op_kwargs={
            'ticker':       TICKER,
            'predict_days': PREDICT_DAYS,
        },
    )
    backfill_task
