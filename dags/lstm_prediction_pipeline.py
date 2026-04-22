# ---------------------------------------------------------------------------
# Two DAGs for LSTM-based stock price prediction
#
#   1. lstm_weekly_training   – runs every Monday at 06:00 UTC
#      train_lstm_model (parallel per ticker)
#
#   2. lstm_daily_prediction  – runs daily when features are ready
#      (after market_momentum_extraction has finished and features are fresh)
#      predict_next_day_close (parallel per ticker)
#
# Both DAGs call helpers in scripts/lstm_model.py.
# ---------------------------------------------------------------------------
import sys
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from datetime import datetime

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Multi-ticker support
# ---------------------------------------------------------------------------
from ticker_config import get_tickers, get_features_ready_dataset_uri

TICKERS = get_tickers()
FEATURES_READY_DATASET = Dataset(get_features_ready_dataset_uri())  # Shared trigger from market_data_pipeline

# ---------------------------------------------------------------------------
# Wrapper callables (lazy-import tensorflow inside the task so the Airflow
# scheduler process doesn't have to load it at DAG-parse time)
# ---------------------------------------------------------------------------

def task_train_model(ticker: str, **context):
    from lstm_model import train_model
    try:
        result = train_model(ticker)
    except ValueError as exc:
        # Skip tickers that do not yet have enough feature history for training.
        if 'Not enough data to train' in str(exc):
            log.warning("Skipping training for %s: %s", ticker, exc)
            raise AirflowSkipException(str(exc))
        raise
    log.info("Training complete: %s", result)
    # Push metrics to XCom so they're visible in the Airflow UI
    context['ti'].xcom_push(key='training_result', value=result)
    return result


def task_predict(ticker: str, **context):
    from lstm_model import predict_next_day
    result = predict_next_day(ticker)
    log.info("Prediction stored: %s", result)
    context['ti'].xcom_push(key='prediction_result', value=result)
    return result


# ---------------------------------------------------------------------------
# DAG 1 – Weekly training
#   Cron: every Monday at 06:00 UTC  →  "0 6 * * 1"
#   Trains models for all tickers in parallel
# ---------------------------------------------------------------------------
with DAG(
    dag_id='lstm_weekly_training',
    description='Train (or retrain) LSTM stock-price models for all tickers every Monday',
    start_date=datetime(2026, 1, 1),        # must be in the past for manual triggers to work
    schedule='0 6 * * 1',                   # every Monday 06:00 UTC
    catchup=False,
    tags=['lstm', 'training', 'weekly'],
    default_args={
        'retries': 1,
    },
) as training_dag:

    train_task = PythonOperator.partial(
        task_id='train_lstm_model',
        python_callable=task_train_model,
    ).expand(op_kwargs=[{'ticker': ticker} for ticker in TICKERS])


# ---------------------------------------------------------------------------
# DAG 2 – Daily prediction
#   Dataset-triggered: runs whenever market_momentum_extraction emits
#   the features-ready dataset from compute_features.
#   Predicts for all tickers in parallel.
# ---------------------------------------------------------------------------
with DAG(
    dag_id='lstm_daily_prediction',
    description='Generate next-day close-price predictions for all tickers using latest LSTM models',
    start_date=datetime(2026, 1, 1),
    schedule=[FEATURES_READY_DATASET],
    catchup=False,
    tags=['lstm', 'prediction', 'daily'],
    default_args={
        'retries': 2,
    },
) as prediction_dag:

    predict_task = PythonOperator.partial(
        task_id='predict_next_day_close',
        python_callable=task_predict,
    ).expand(op_kwargs=[{'ticker': ticker} for ticker in TICKERS])
