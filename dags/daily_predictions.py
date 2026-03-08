from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pickle
import numpy as np
import pandas as pd
import os

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'airflow')
DB_PORT = os.getenv('DB_PORT', '5432')


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )


# Fetch the latest stock data from the stock_prices table for a specific date (e.g yesterday)
def fetch_latest_stock_data(ticker, **context):
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT open, high, low, close, volume FROM stock_prices 
        WHERE ticker = %s AND date = %s
    """
    cursor.execute(query, (ticker, date_str))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return [(ticker, date_str, *row)]
    else:
        print(f"No data for {ticker} on {date_str}.")
        return None

# Make Predictions using the pre-trained model
def make_predictions(ticker, **context):
    data = fetch_latest_stock_data(ticker)
    return None


def upsert_prediction(ticker, date_str, predicted_close):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO model_predictions (ticker, predicted_date, predicted_close, model_version)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker, predicted_date, model_version) DO UPDATE
        SET predicted_close = EXCLUDED.predicted_close
    """
    cursor.execute(query, (ticker, date_str, predicted_close, "v1.0"))
    conn.commit()
    cursor.close()
    conn.close()

# DAG Definition
with DAG(
    dag_id='daily_stock_prediction',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    description='Daily prediction for stock prices using pre-trained model'
) as dag:

    daily_prediction_task = PythonOperator(
        task_id='make_daily_prediction',
        python_callable=make_predictions,
        op_kwargs={'ticker': 'AAPL'}
    )