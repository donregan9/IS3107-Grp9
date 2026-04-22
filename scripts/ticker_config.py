"""Shared ticker configuration for Airflow DAGs.

This module keeps the supported ticker list in one place so the ingestion,
backfill, and LSTM DAGs stay aligned.
"""

import os


DEFAULT_TICKERS = ('AAPL', 'NVDA', 'MSFT')
FEATURES_READY_DATASET_URI = 'dataset://stock_features/ready'


def get_tickers():
    """Return the configured ticker list.

    If PROJECT_TICKERS is set, it should contain a comma-separated list such as
    "AAPL,NVDA". Otherwise the default list is used.
    """
    configured_value = os.getenv('PROJECT_TICKERS', '').strip()
    if not configured_value:
        return list(DEFAULT_TICKERS)

    tickers = [ticker.strip().upper() for ticker in configured_value.split(',') if ticker.strip()]
    return tickers or list(DEFAULT_TICKERS)


def get_features_ready_dataset_uri():
    """Return the shared dataset URI used to trigger feature-dependent DAGs."""
    return FEATURES_READY_DATASET_URI