import os
import json
import urllib.parse
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPERSET_URL = "http://localhost:8089"
USERNAME     = "admin"
PASSWORD     = "admin"

TARGET_DASHBOARD_NAME = "StockSight Prediction Dashboard" 

ZIP_PATH = "../superset/exports/stockSight.zip"

DB_PASSWORDS = {
    "airflow": "admin" 
}

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
def create_session():
    print("--- Authenticating ---")
    session = requests.Session()

    # 1. Get Access Token
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    # 2. Get CSRF Token
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    csrf_token = resp.json()["result"]

    # 3. Update Session Headers
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "X-CSRFToken":   csrf_token,
        "Referer":       SUPERSET_URL,
    })

    print(" ✓ Authenticated with Superset")
    return session

# ---------------------------------------------------------------------------
# Pre-Import Cleanup (Delete by Name)
# ---------------------------------------------------------------------------
def delete_existing_dashboards(session, dashboard_name):
    print(f"\n--- Searching for existing dashboards named '{dashboard_name}' ---")
    
    query_payload = {
        "filters": [{"col": "dashboard_title", "opr": "eq", "value": dashboard_name}]
    }
    
    encoded_query = urllib.parse.quote(json.dumps(query_payload))
    search_url = f"{SUPERSET_URL}/api/v1/dashboard/?q={encoded_query}"
    
    search_resp = session.get(search_url)
    
    if not search_resp.ok:
        print(f" ✗ Error searching for dashboards: {search_resp.status_code} - {search_resp.text}")
        return

    dashboards = search_resp.json().get("result", [])
    
    if not dashboards:
        print(" ✓ No duplicates found. Safe to proceed.")
        return

    print(f" ! Found {len(dashboards)} existing dashboard(s). Deleting now...")
    
    for dash in dashboards:
        dash_id = dash["id"]
        delete_url = f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}"
        
        del_resp = session.delete(delete_url)
        if del_resp.ok:
            print(f"   ✓ Deleted dashboard ID {dash_id}")
        else:
            print(f"   ✗ Failed to delete ID {dash_id}: {del_resp.status_code}")

# ---------------------------------------------------------------------------
# Import Dashboard
# ---------------------------------------------------------------------------
def import_dashboard(session):
    print("\n--- Importing Dashboard ---")
    
    if not os.path.exists(ZIP_PATH):
        print(f" ✗ Error: Could not find ZIP file at '{ZIP_PATH}'")
        return

    url = f"{SUPERSET_URL}/api/v1/dashboard/import/"
    
    with open(ZIP_PATH, "rb") as f:
        files = {
            "formData": (os.path.basename(ZIP_PATH), f, "application/zip")
        }
        data = {
            "overwrite": "true", 
            "passwords": json.dumps(DB_PASSWORDS)
        }
        
        resp = session.post(url, files=files, data=data)

    if resp.ok:
        print(" ✓ Successfully imported the dashboard!")
        print(" ✓ Datasets, charts, filters, and layout have been fully restored.")
    else:
        print(f" ✗ Import failed: {resp.status_code}")
        print(f"   {resp.text}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=== Superset Dashboard Auto-Importer ===\n")
    session = create_session()
    
    # 1. Delete Clone Dashboard
    delete_existing_dashboards(session, TARGET_DASHBOARD_NAME)
    
    # 2. Import finished dashboard
    import_dashboard(session)
    
    print("\n=== Done ===")

if __name__ == "__main__":
    main()