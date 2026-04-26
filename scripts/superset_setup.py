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

Superset must be running at 127.0.0.1:8089.
Every run does a full teardown + recreate — safe to run repeatedly.
"""

import re
import json
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPERSET_URL    = "http://127.0.0.1:8089"
USERNAME        = "admin"
PASSWORD        = "admin"
DB_NAME         = "airflow"
SQL_FILE        = "./superset_dashboard_queries.sql"
DASHBOARD_TITLE = "StockSight Prediction Dashboard"

# ---------------------------------------------------------------------------
# Chart config: name, viz_type, and full params (x-axis + metrics)
# ---------------------------------------------------------------------------
CHART_CONFIG = {
    1: {
        "name":     "Predicted vs Actual Close",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",             
            "granularity_sqla": "ds",    
            "time_range": "No filter",   
            "metrics": [
                {"label": "Predicted", "expressionType": "SIMPLE", "column": {"column_name": "predicted_close"}, "aggregate": "MAX"},
                {"label": "Actual",    "expressionType": "SIMPLE", "column": {"column_name": "actual_close"},    "aggregate": "MAX"},
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
            "seriesType": "line",
            "show_legend": True,
            "rich_tooltip": True,
            "y_axis_title": "Price ($)",
        },
    },
    2: {
        "name": "Rolling 7-Day Directional Accuracy",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {
                    "label": "Hit Rate", 
                    "expressionType": "SIMPLE", 
                    "column": {"column_name": "rolling_7d_directional_accuracy"}, 
                    "aggregate": "MAX" 
                },
            ],
            "y_axis_format": ".1%", 
            
            "y_axis_bounds": [0, 1], 
            
            # Optional UI Polish
            "show_legend": False,
            "seriesType": "line",
            "rich_tooltip": True,
            "color_picker": {"r": 255, "g": 165, "b": 0, "a": 1}, # Orange line for distinction
        },
    },
    3: {
    "name": "DAG Reliability",
    "viz_type": "echarts_timeseries_bar",
    "params": {
        "groupby": ["state"],
        "metrics": [
            {
                "label": "run_count",
                "expressionType": "SIMPLE",
                "column": {"column_name": "run_count"},
                "aggregate": "SUM"
            }
        ],
        "x_axis": "dag_id",
        "stack": "Stack",
        "color_scheme": "d3Category10"
    }
    },
    4: {
        "name":     "Ticker",
        "viz_type": "table",
        "params": {
            "all_columns": ["Stock Ticker", "Last Updated"],
            "groupby": [],
            "row_limit": 1000,
            "include_search": False,
        },
    },
    5: {
        "name":     "Daily Prediction Residual Error",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "ds",
            "metrics": [
                {
                    "label": "Residual Error", 
                    "expressionType": "SIMPLE", 
                    "column": {"column_name": "residual_error"}, 
                    "aggregate": "AVG"
                },
            ],
            "groupby": [],
            "time_grain_sqla": "P1D",
            "y_axis_title": "Price Difference ($)",
            "show_legend": True,

            "seriesType": "bar",
            "opacity": 0.7,
        },
    },
    6: {
        "name":     "Prediction Error Distribution",
        "viz_type": "histogram_v2",
        "params": {
            "column": "prediction_error", 
            "bins": 25,
            "query_mode": "raw",
            "time_range": "No filter",
            "x_axis_title": "Error ($)", 
            "y_axis_title": "Frequency",
            "adhoc_filters": [],
            "row_limit": 1000,
            "color_scheme": "supersetColors",
            "show_legend": False,
        "name":     "Latest Price Date",
        "viz_type": "table",
        "params": {
            "all_columns": ["ticker", "latest_price_date"],
            "groupby": [],
            "row_limit": 1000,
        },
        },
    },
    7: {
        "name": "Close",
        "viz_type": "big_number_total",
        "params": {
            "metric": {
                "label": "Close",
                "expressionType": "SIMPLE",
                "column": {"column_name": "close"},
                "aggregate": "MAX"
            },
            "y_axis_format": "$,.2f",
            "subheader_fontsize": 0.8,
        },
    },
    8: {
        "name": "Predicted Next Close",
        "viz_type": "big_number_total", 
        "params": {
            "metric": {
                "label": "Predicted",
                "expressionType": "SIMPLE",
                "column": {"column_name": "predicted_close"},
                "aggregate": "MAX"
            },
            "y_axis_format": "$,.2f",
            "conditional_formatting": [
                {
                    "colorScheme": "#28A745", 
                    "column": "Predicted",    
                    "operator": ">",
                    "targetValue": -999999    
                }
            ]
        },
    },
    9: {
        "name": "Model Performance Metrics",
        "viz_type": "table",
        "params": {
            "all_columns": ["Model Version", "MAE ($)", "RMSE ($)", "Creation Date"],
            "groupby": [],
            "row_limit": 100,
            "include_search": False,
            "order_by_cols": ["[\"Creation Date\", false]"],
        },
    },

}

# Collect all chart/dataset names for teardown matching
ALL_NAMES = {cfg["name"] for cfg in CHART_CONFIG.values()}.union({"ticker_filters"})

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
    # 1. Inject the 'glue' fields into the params dictionary before stringifying
    params["datasource"] = f"{dataset_id}__table"
    params["viz_type"] = viz_type
    
    payload = {
        "slice_name":      name,
        "viz_type":        viz_type,
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params":          json.dumps(params), # This now contains the injected keys
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
    return dashboard_id

# ---------------------------------------------------------------------------
# Dashboard Filter
# ---------------------------------------------------------------------------

def add_native_filter_to_dashboard(session, dashboard_id, filter_dataset_id, exclude_chart_ids):
    print("\n--- Configuring Dashboard Native Filter ---")
    
    # 1. Fetch current dashboard config
    resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}")
    if not resp.ok:
        print(f" ✗ Failed to fetch dashboard {dashboard_id}: {resp.text}")
        return
        
    dash_data = resp.json()["result"]
    
    # 2. Parse the hidden json_metadata (where filters live)
    raw_metadata = dash_data.get("json_metadata") or "{}"
    json_meta = json.loads(raw_metadata)
    
    # 3. Define the filter architecture
    filter_config = {
        "id": "NATIVE_FILTER-ticker_select",
        "name": "Ticker",
        "filterType": "filter_select",
        "targets": [{"datasetId": filter_dataset_id, "column": {"name": "ticker"}}],
        "controlValues": {
            "enableEmptyFilter": False,
            "multiSelect": False,
            "inverseSelection": False
        },
        "defaultDataMask": {
            "filterState": {"value": ["AAPL"]}
        },
        "scope": {
            "rootPath": ["ROOT_ID"],
            "excluded": exclude_chart_ids # Skips the DAG charts
        }
    }
    
    # 4. Inject into metadata
    json_meta["native_filter_configuration"] = [filter_config]
    
    # 5. Push the updated metadata back to Superset
    put_payload = {
        "json_metadata": json.dumps(json_meta)
    }
    
    put_resp = session.put(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}", json=put_payload)
    if put_resp.ok:
        print(" ✓ Successfully injected 'Ticker' Native Filter (Default: AAPL)")
    else:
        print(f" ✗ Failed to update filter metadata: {put_resp.text}")


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
    exclude_from_filter = [] # Exclude Dag Related Data

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
            if num in [3]:
                exclude_from_filter.append(chart_id)

    filter_sql = "SELECT unnest(ARRAY['AAPL', 'MSFT', 'NVDA']) AS ticker;"
    filter_dataset_id = create_dataset(session, db_id, "ticker_filters", filter_sql)

    print("\n=== Creating Dashboard ===")
    dashboard_id = create_dashboard(session, chart_ids, chart_names)
    print("\n✓ All done!")

    if dashboard_id and filter_dataset_id:
        add_native_filter_to_dashboard(session, dashboard_id, filter_dataset_id, exclude_from_filter)


if __name__ == "__main__":
    main()