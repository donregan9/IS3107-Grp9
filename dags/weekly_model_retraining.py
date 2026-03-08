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


def fetch_historical_stock_data(ticker, start_date, end_date, **context):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT date, open, high, low, close, volume 
        FROM stock_prices 
        WHERE ticker = %s AND date BETWEEN %s AND %s
        ORDER BY date ASC
    """
    cursor.execute(query, (ticker, start_date, end_date))
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

# Retrain the model using historical data from stock_prices table
def retrain_model(ticker, **context):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=52)  # Last years

    historical_data = fetch_historical_stock_data(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    if not historical_data:
        print(f"No data found for {ticker} in the given date range.")
        return

    # Train model heres

    print(f"Model retrained.")


# DAG Definition
with DAG(
    dag_id='weekly_model_retraining',
    start_date=datetime(2026, 1, 1),
    schedule='@weekly',
    catchup=False,
    description='Weekly retraining of the stock prediction model'
) as dag:

    retrain_model_task = PythonOperator(
        task_id='retrain_stock_prediction_model',
        python_callable=retrain_model,
        op_kwargs={'ticker': 'AAPL'}
    )