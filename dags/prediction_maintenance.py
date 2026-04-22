"""Daily prediction maintenance DAG.

This DAG replaces the separate recent-backfill and missed-prediction DAGs.
It runs every day, refreshes the most recent prediction window, and then fills
any missing historical predictions or missing actual values.
"""

import logging
import os
import pickle
import sys
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from ticker_config import get_tickers


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'airflow')
DB_PORT = os.getenv('DB_PORT', '5432')

TICKERS = get_tickers()
SEQUENCE_LEN = 30
PREDICT_DAYS = 14

FEATURE_COLS = [
    'close', 'daily_return', 'sma_20', 'sma_50',
    'ema_12', 'ema_26', 'macd', 'macd_signal',
    'rsi_14', 'bb_upper', 'bb_lower', 'volatility_14', 'volume_sma_20',
    'bb_width', 'volume_ratio_20', 'return_3d', 'return_5d'
]


# ---------------------------------------------------------------------------
# DB and model helpers
# ---------------------------------------------------------------------------
def _get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
    )


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
    path = _version_path(ticker)
    if os.path.exists(path):
        with open(path) as file_handle:
            return file_handle.read().strip()
    return None


def _next_trading_day(dt) -> date:
    if hasattr(dt, 'date'):
        dt = dt.date()
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day


def _load_artifacts(ticker: str):
    from tensorflow.keras.models import load_model

    version = _current_version(ticker)
    if version is None:
        raise AirflowSkipException(f"No trained model found for {ticker}.")

    model_path = _model_path(ticker)
    scaler_path = _scaler_path(ticker)
    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        raise AirflowSkipException(
            f"Missing model artifacts for {ticker}. Run lstm_weekly_training first."
        )

    model = load_model(model_path)
    with open(scaler_path, 'rb') as file_handle:
        scaler = pickle.load(file_handle)

    return version, model, scaler


def _fetch_feature_history(ticker: str) -> pd.DataFrame:
    conn = _get_conn()
    try:
        return pd.read_sql(
            f"SELECT date, {', '.join(FEATURE_COLS)} FROM stock_features WHERE ticker = %s ORDER BY date ASC",
            conn,
            params=(ticker,),
        )
    finally:
        conn.close()


def _fetch_actuals(ticker: str) -> dict:
    conn = _get_conn()
    try:
        actuals_df = pd.read_sql(
            "SELECT DATE(date) AS date, close FROM stock_prices WHERE ticker = %s",
            conn,
            params=(ticker,),
        )
    finally:
        conn.close()

    if actuals_df.empty:
        return {}

    actuals_df['date'] = pd.to_datetime(actuals_df['date']).dt.date
    return dict(zip(actuals_df['date'], actuals_df['close']))


def _fetch_existing_predictions(ticker: str) -> dict:
    conn = _get_conn()
    try:
        existing_df = pd.read_sql(
            "SELECT predicted_date, predicted_close, actual_close FROM model_predictions WHERE ticker = %s",
            conn,
            params=(ticker,),
        )
    finally:
        conn.close()

    if existing_df.empty:
        return {}

    existing_df['predicted_date'] = pd.to_datetime(existing_df['predicted_date']).dt.date
    return {
        row['predicted_date']: {
            'predicted_close': row['predicted_close'],
            'actual_close': row['actual_close'],
        }
        for _, row in existing_df.iterrows()
    }


def _upsert_predictions(records):
    if not records:
        return

    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'model_predictions_ticker_predicted_date_key'
                ) THEN
                    ALTER TABLE model_predictions
                    ADD CONSTRAINT model_predictions_ticker_predicted_date_key
                    UNIQUE (ticker, predicted_date);
                END IF;
            END $$;
            """
        )
        execute_values(
            cursor,
            """
            INSERT INTO model_predictions
                (ticker, predicted_date, predicted_close, actual_close, model_version)
            VALUES %s
            ON CONFLICT (ticker, predicted_date) DO UPDATE SET
                predicted_close = COALESCE(model_predictions.predicted_close, EXCLUDED.predicted_close),
                actual_close = COALESCE(model_predictions.actual_close, EXCLUDED.actual_close),
                model_version = COALESCE(model_predictions.model_version, EXCLUDED.model_version)
            """,
            records,
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------
def maintain_predictions(ticker: str, predict_days: int, **context):
    version, model, scaler = _load_artifacts(ticker)

    df = _fetch_feature_history(ticker)
    if df.empty or len(df) < SEQUENCE_LEN:
        raise AirflowSkipException(
            f"Not enough feature history for {ticker}: need at least {SEQUENCE_LEN} rows, have {len(df)}."
        )

    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values('date').reset_index(drop=True)

    actuals = _fetch_actuals(ticker)
    existing_predictions = _fetch_existing_predictions(ticker)

    scaled = scaler.transform(df[FEATURE_COLS].values)
    recent_window_start = max(SEQUENCE_LEN - 1, len(df) - predict_days)

    records = []
    recent_rows = 0
    gap_rows = 0
    actual_updates = 0

    for index in range(SEQUENCE_LEN - 1, len(df)):
        window_end_date = df['date'].iloc[index]
        predicted_date = _next_trading_day(window_end_date)
        existing_row = existing_predictions.get(predicted_date)
        existing_predicted = None if existing_row is None else existing_row.get('predicted_close')
        existing_actual = None if existing_row is None else existing_row.get('actual_close')

        is_recent_window = index >= recent_window_start
        missing_prediction = existing_row is None or pd.isna(existing_predicted)
        missing_actual = existing_row is not None and pd.isna(existing_actual)

        # Maintenance behavior:
        # 1) Fill missing predicted_close values only (never overwrite existing predictions).
        # 2) Fill missing actual_close values for existing rows when actuals become available.
        if not missing_prediction and not missing_actual:
            continue

        actual_close = actuals.get(predicted_date)

        if missing_prediction:
            window = scaled[index - SEQUENCE_LEN + 1:index + 1, :].reshape(1, SEQUENCE_LEN, len(FEATURE_COLS))
            raw_prediction = model.predict(window, verbose=0)[0, 0]

            pad = np.zeros((1, len(FEATURE_COLS)))
            pad[0, 0] = raw_prediction
            predicted_close = float(scaler.inverse_transform(pad)[0, 0])

            if is_recent_window:
                recent_rows += 1
            else:
                gap_rows += 1
        else:
            predicted_close = float(existing_predicted)
            actual_updates += 1

        records.append((ticker, predicted_date, predicted_close, actual_close, version))

    if not records:
        log.info("No prediction maintenance required for %s.", ticker)
        result = {
            'status': 'success',
            'ticker': ticker,
            'records': 0,
            'recent_rows': 0,
            'gap_rows': 0,
            'actual_updates': 0,
            'comparable': 0,
            'mae_price': None,
            'rmse_price': None,
            'model_version': version,
            'predict_days': predict_days,
        }
        context['ti'].xcom_push(key='prediction_maintenance_result', value=result)
        return result

    _upsert_predictions(records)

    comparable = [
        (predicted_close, actual_close)
        for _, _, predicted_close, actual_close, _ in records
        if actual_close is not None and not pd.isna(actual_close)
    ]
    if comparable:
        preds = np.array([pred for pred, _ in comparable])
        actual_values = np.array([actual for _, actual in comparable])
        mae = float(np.mean(np.abs(preds - actual_values)))
        rmse = float(np.sqrt(np.mean((preds - actual_values) ** 2)))
        log.info(
            "%s prediction maintenance: %d rows (%d recent, %d gap fills) | %d comparable | MAE: $%.4f | RMSE: $%.4f",
            ticker,
            len(records),
            recent_rows,
            gap_rows,
            len(comparable),
            mae,
            rmse,
        )
    else:
        mae = None
        rmse = None
        log.info(
            "%s prediction maintenance: %d rows (%d recent, %d gap fills, %d actual updates) | no comparable actuals yet.",
            ticker,
            len(records),
            recent_rows,
            gap_rows,
            actual_updates,
        )

    result = {
        'status': 'success',
        'ticker': ticker,
        'records': len(records),
        'recent_rows': recent_rows,
        'gap_rows': gap_rows,
        'actual_updates': actual_updates,
        'comparable': len(comparable),
        'mae_price': mae,
        'rmse_price': rmse,
        'model_version': version,
        'predict_days': predict_days,
    }
    context['ti'].xcom_push(key='prediction_maintenance_result', value=result)
    return result


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='prediction_maintenance',
    description='Daily prediction maintenance: refresh recent forecasts and fill gaps',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['lstm', 'backfill', 'evaluation', 'maintenance'],
    default_args={'retries': 1},
) as dag:

    maintain_task = PythonOperator.partial(
        task_id='maintain_predictions',
        python_callable=maintain_predictions,
    ).expand(
        op_kwargs=[{'ticker': ticker, 'predict_days': PREDICT_DAYS} for ticker in TICKERS]
    )

    maintain_task