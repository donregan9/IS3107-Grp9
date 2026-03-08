# ---------------------------------------------------------------------------
# Two DAGs for LSTM-based stock price prediction
#
#   1. lstm_weekly_training   – runs every Monday at 06:00 UTC
#      create_table → train_lstm_model
#
#   2. lstm_daily_prediction  – runs daily at 01:00 UTC
#      (after market_momentum_extraction has finished and features are fresh)
#      predict_next_day_close → log_prediction
#
# Both DAGs call helpers in scripts/lstm_model.py.
# ---------------------------------------------------------------------------
import sys
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

log = logging.getLogger(__name__)

TICKER = 'AAPL'   # change or extend to a list as needed

# ---------------------------------------------------------------------------
# Wrapper callables (lazy-import tensorflow inside the task so the Airflow
# scheduler process doesn't have to load it at DAG-parse time)
# ---------------------------------------------------------------------------

def task_train_model(ticker: str, **context):
    from lstm_model import train_model
    result = train_model(ticker)
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
# ---------------------------------------------------------------------------
with DAG(
    dag_id='lstm_weekly_training',
    description='Train (or retrain) the LSTM stock-price model every Monday',
    start_date=datetime(2026, 1, 1),        # must be in the past for manual triggers to work
    schedule='0 6 * * 1',                   # every Monday 06:00 UTC
    catchup=False,
    tags=['lstm', 'training', 'weekly'],
    default_args={
        'retries': 1,
    },
) as training_dag:

    train_task = PythonOperator(
        task_id='train_lstm_model',
        python_callable=task_train_model,
        op_kwargs={'ticker': TICKER},
    )


# ---------------------------------------------------------------------------
# DAG 2 – Daily prediction
#   Cron: every day at 01:00 UTC (after midnight data fetch + feature run)
# ---------------------------------------------------------------------------
with DAG(
    dag_id='lstm_daily_prediction',
    description='Generate next-day close-price prediction using the latest LSTM model',
    start_date=datetime(2026, 1, 1),
    schedule='0 1 * * 1-5',                 # Mon–Fri at 01:00 UTC (trading days only)
    catchup=False,
    tags=['lstm', 'prediction', 'daily'],
    default_args={
        'retries': 2,
    },
) as prediction_dag:

    predict_task = PythonOperator(
        task_id='predict_next_day_close',
        python_callable=task_predict,
        op_kwargs={'ticker': TICKER},
    )
