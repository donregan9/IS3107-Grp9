"""
superset_setup.py
-----------------
Reads superset_dashboard_queries.sql and automatically:
  1. Logs into Superset and gets an auth token
  2. Finds the airflow PostgreSQL database connection ID
  3. DELETES any existing charts, datasets, dashboards with matching names
  4. Creates a virtual dataset (saved SQL query) for each numbered query
  5. Creates a chart for each dataset with x-axis and metrics pre-configured
  6. Creates an empty dashboard and prints instructions to add charts

Run this script from your local machine (outside Docker):
    python superset_setup.py

Requirements:
    pip install requests

Superset must be running at localhost:8089.
Every run does a full teardown + recreate — safe to run repeatedly.
"""

import re
import json
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPERSET_URL    = "http://localhost:8089"
USERNAME        = "admin"
PASSWORD        = "admin"
DB_NAME         = "airflow"
SQL_FILE        = "./superset_dashboard_queries.sql"
DASHBOARD_TITLE = "AAPL Stock Prediction Dashboard"

# ---------------------------------------------------------------------------
# Chart config: name, viz_type, and full params (x-axis + metrics)
# ---------------------------------------------------------------------------
CHART_CONFIG = {
    1: {
        "name":     "Price Trend with Moving Averages",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "close",  "expressionType": "SIMPLE", "column": {"column_name": "close"},  "aggregate": "MAX"},
                {"label": "sma_20", "expressionType": "SIMPLE", "column": {"column_name": "sma_20"}, "aggregate": "MAX"},
                {"label": "sma_50", "expressionType": "SIMPLE", "column": {"column_name": "sma_50"}, "aggregate": "MAX"},
                {"label": "ema_12", "expressionType": "SIMPLE", "column": {"column_name": "ema_12"}, "aggregate": "MAX"},
                {"label": "ema_26", "expressionType": "SIMPLE", "column": {"column_name": "ema_26"}, "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    2: {
        "name":     "Candlestick OHLCV",
        "viz_type": "echarts_candlestick",
        "params": {
            "x_axis": "ds",
            "open":   {"label": "open",  "expressionType": "SIMPLE", "column": {"column_name": "open"},  "aggregate": "MAX"},
            "close":  {"label": "close", "expressionType": "SIMPLE", "column": {"column_name": "close"}, "aggregate": "MAX"},
            "high":   {"label": "high",  "expressionType": "SIMPLE", "column": {"column_name": "high"},  "aggregate": "MAX"},
            "low":    {"label": "low",   "expressionType": "SIMPLE", "column": {"column_name": "low"},   "aggregate": "MAX"},
            "time_grain_sqla": "P1D",
        },
    },
    3: {
        "name":     "RSI Momentum",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "rsi_14", "expressionType": "SIMPLE", "column": {"column_name": "rsi_14"}, "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    4: {
        "name":     "MACD vs Signal",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "macd",        "expressionType": "SIMPLE", "column": {"column_name": "macd"},        "aggregate": "MAX"},
                {"label": "macd_signal", "expressionType": "SIMPLE", "column": {"column_name": "macd_signal"}, "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    5: {
        "name":     "Bollinger Bands",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "close",    "expressionType": "SIMPLE", "column": {"column_name": "close"},    "aggregate": "MAX"},
                {"label": "bb_upper", "expressionType": "SIMPLE", "column": {"column_name": "bb_upper"}, "aggregate": "MAX"},
                {"label": "bb_lower", "expressionType": "SIMPLE", "column": {"column_name": "bb_lower"}, "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    6: {
        "name":     "Predicted vs Actual Close",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "predicted_close", "expressionType": "SIMPLE", "column": {"column_name": "predicted_close"}, "aggregate": "MAX"},
                {"label": "actual_close",    "expressionType": "SIMPLE", "column": {"column_name": "actual_close"},    "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    7: {
        "name":     "Prediction Quality by Model Version",
        "viz_type": "echarts_bar",
        "params": {
            "groupby": ["model_version"],
            "metrics": [
                {"label": "mae",  "expressionType": "SIMPLE", "column": {"column_name": "mae"},  "aggregate": "MAX"},
                {"label": "rmse", "expressionType": "SIMPLE", "column": {"column_name": "rmse"}, "aggregate": "MAX"},
            ],
        },
    },
    8: {
        "name":     "Rolling 7D Directional Accuracy",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {"label": "rolling_7d_directional_accuracy", "expressionType": "SIMPLE", "column": {"column_name": "rolling_7d_directional_accuracy"}, "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
        },
    },
    9: {
        "name":     "Daily Return Distribution",
        "viz_type": "Histogram",
        "params": {
            "all_columns": ["daily_return"],
            "groupby": [],
        },
    },
    10: {
        "name":     "Pipeline Freshness Scorecard",
        "viz_type": "table",
        "params": {
            "all_columns": [
                "latest_price_date",
                "latest_feature_date",
                "latest_prediction_date",
                "checked_at",
            ],
            "groupby": [],
        },
    },
    11: {
        "name":     "DAG Reliability",
        "viz_type": "echarts_bar",
        "params": {
            "groupby": ["dag_id", "state"],
            "metrics": [
                {"label": "run_count", "expressionType": "SIMPLE", "column": {"column_name": "run_count"}, "aggregate": "SUM"},
            ],
        },
    },
    12: {
        "name":     "DAG Run Duration Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "run_date",
            "metrics": [
                {"label": "duration_minutes", "expressionType": "SIMPLE", "column": {"column_name": "duration_minutes"}, "aggregate": "MAX"},
            ],
            "groupby": ["dag_id"],
            "time_grain_sqla": "P1D",
        },
    },
}

# Collect all chart/dataset names for teardown matching
ALL_NAMES = {cfg["name"] for cfg in CHART_CONFIG.values()}

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def create_session():
    session = requests.Session()

    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    resp = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    csrf_token = resp.json()["result"]

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "X-CSRFToken":   csrf_token,
        "Content-Type":  "application/json",
        "Referer":       SUPERSET_URL,
    })

    print("✓ Authenticated with Superset")
    return session


# ---------------------------------------------------------------------------
# Teardown — delete matching dashboards, charts, datasets
# ---------------------------------------------------------------------------
def teardown(session):
    print("\n--- Teardown: deleting existing resources ---")

    # Delete matching dashboards
    resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/?q=(page_size:100)")
    if resp.ok:
        for d in resp.json().get("result", []):
            if d["dashboard_title"] == DASHBOARD_TITLE:
                r = session.delete(f"{SUPERSET_URL}/api/v1/dashboard/{d['id']}")
                print(f"  🗑 Deleted dashboard '{d['dashboard_title']}' (ID {d['id']})" if r.ok else f"  ⚠ Could not delete dashboard {d['id']}: {r.text}")

    # Delete matching charts
    resp = session.get(f"{SUPERSET_URL}/api/v1/chart/?q=(page_size:100)")
    if resp.ok:
        for c in resp.json().get("result", []):
            if c["slice_name"] in ALL_NAMES:
                r = session.delete(f"{SUPERSET_URL}/api/v1/chart/{c['id']}")
                print(f"  🗑 Deleted chart '{c['slice_name']}' (ID {c['id']})" if r.ok else f"  ⚠ Could not delete chart {c['id']}: {r.text}")

    # Delete matching datasets
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/?q=(page_size:100)")
    if resp.ok:
        for d in resp.json().get("result", []):
            if d["table_name"] in ALL_NAMES:
                r = session.delete(f"{SUPERSET_URL}/api/v1/dataset/{d['id']}")
                print(f"  🗑 Deleted dataset '{d['table_name']}' (ID {d['id']})" if r.ok else f"  ⚠ Could not delete dataset {d['id']}: {r.text}")

    print("--- Teardown complete ---\n")


# ---------------------------------------------------------------------------
# Find database ID
# ---------------------------------------------------------------------------
def get_database_id(session):
    resp = session.get(f"{SUPERSET_URL}/api/v1/database/")
    resp.raise_for_status()
    for db in resp.json()["result"]:
        if db["database_name"] == DB_NAME:
            print(f"✓ Found database '{DB_NAME}' with ID {db['id']}")
            return db["id"]
    raise ValueError(
        f"Database '{DB_NAME}' not found in Superset. "
        "Add the PostgreSQL connection in Settings → Database Connections first."
    )


# ---------------------------------------------------------------------------
# Parse SQL file
# ---------------------------------------------------------------------------
def parse_sql_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()

    pattern = r"--\s*(\d+)\)\s[^\n]+"
    parts   = re.split(pattern, content)

    queries = {}
    for i in range(1, len(parts) - 1, 2):
        num = int(parts[i])
        sql = parts[i + 1].strip()
        sql_lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
        sql = "\n".join(sql_lines).strip()
        if sql:
            queries[num] = sql

    print(f"✓ Parsed {len(queries)} queries from {filepath}")
    return queries


# ---------------------------------------------------------------------------
# Create dataset
# ---------------------------------------------------------------------------
def create_dataset(session, database_id, name, sql):
    payload = {
        "database":              database_id,
        "schema":                "public",
        "sql":                   sql,
        "table_name":            name,
        "is_managed_externally": False,
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload)
    if not resp.ok:
        print(f"  ✗ Failed to create dataset '{name}': {resp.status_code} — {resp.text}")
        return None
    dataset_id = resp.json()["id"]
    print(f"  ✓ Created dataset '{name}' (ID {dataset_id})")
    return dataset_id


# ---------------------------------------------------------------------------
# Create chart
# ---------------------------------------------------------------------------
def create_chart(session, name, viz_type, params, dataset_id):
    payload = {
        "slice_name":      name,
        "viz_type":        viz_type,
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params":          json.dumps(params),
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload)
    if not resp.ok:
        print(f"  ✗ Failed to create chart '{name}': {resp.status_code} — {resp.text}")
        return None
    chart_id = resp.json()["id"]
    print(f"  ✓ Created chart '{name}' (ID {chart_id})")
    return chart_id


# ---------------------------------------------------------------------------
# Create dashboard
# ---------------------------------------------------------------------------
def create_dashboard(session, chart_ids, chart_names):
    valid = [(cid, name) for cid, name in zip(chart_ids, chart_names) if cid is not None]

    if not valid:
        print("⚠ No charts were created — skipping dashboard creation.")
        return

    resp = session.post(
        f"{SUPERSET_URL}/api/v1/dashboard/",
        json={"dashboard_title": DASHBOARD_TITLE, "published": True},
    )
    if not resp.ok:
        print(f"✗ Failed to create dashboard: {resp.status_code} — {resp.text}")
        return

    dashboard_id = resp.json()["id"]
    print(f"\n✓ Created dashboard '{DASHBOARD_TITLE}' (ID {dashboard_id})")
    print(f"\n{'='*60}")
    print("NEXT STEP: Add charts manually (takes ~1 minute)")
    print(f"{'='*60}")
    print(f"1. Open: {SUPERSET_URL}/superset/dashboard/{dashboard_id}/edit")
    print(f"2. Click the 'Charts' tab on the left panel")
    print(f"3. Drag and drop each chart onto the canvas:")
    for cid, name in valid:
        print(f"     • {name}  (ID {cid})")
    print(f"4. Click 'Save' when done")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=== Superset Auto-Setup ===\n")

    session = create_session()

    # Always teardown first so re-runs start clean
    teardown(session)

    db_id       = get_database_id(session)
    queries     = parse_sql_file(SQL_FILE)
    chart_ids   = []
    chart_names = []

    for num, sql in sorted(queries.items()):
        config = CHART_CONFIG.get(num)
        if not config:
            print(f"⚠ No chart config for query {num} — skipping.")
            continue

        print(f"\n[Query {num}] {config['name']}")
        dataset_id = create_dataset(session, db_id, config["name"], sql)
        if dataset_id:
            chart_id = create_chart(
                session,
                config["name"],
                config["viz_type"],
                config["params"],
                dataset_id,
            )
            chart_ids.append(chart_id)
            chart_names.append(config["name"])

    print("\n=== Creating Dashboard ===")
    create_dashboard(session, chart_ids, chart_names)
    print("\n✓ All done!")


if __name__ == "__main__":
    main()