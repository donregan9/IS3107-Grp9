import os
import json
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPERSET_URL = "http://localhost:8089"
USERNAME     = "admin"
PASSWORD     = "admin"

# The path to the ZIP file you exported from the Superset UI
ZIP_PATH = "../superset/exports/stockSight.zip"

# Superset needs the database password to reconnect your 'airflow' database 
# during the import process. Replace 'your_db_password' if it is different.
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
# Import Dashboard
# ---------------------------------------------------------------------------
def import_dashboard(session):
    print("\n--- Importing Dashboard ---")
    
    if not os.path.exists(ZIP_PATH):
        print(f" ✗ Error: Could not find ZIP file at '{ZIP_PATH}'")
        return

    url = f"{SUPERSET_URL}/api/v1/dashboard/import/"
    
    # Superset requires the ZIP to be sent as multipart/form-data.
    # The 'requests' library handles the Content-Type automatically when using the 'files' parameter.
    with open(ZIP_PATH, "rb") as f:
        files = {
            "formData": (os.path.basename(ZIP_PATH), f, "application/zip")
        }
        data = {
            "overwrite": "true", # Allows you to re-run the script safely
            "passwords": json.dumps(DB_PASSWORDS)
        }
        
        # Note: Do not manually set Content-Type to multipart/form-data; 
        # requests will do it and append the necessary boundary string.
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
    import_dashboard(session)
    print("\n=== Done ===")

if __name__ == "__main__":
    main()