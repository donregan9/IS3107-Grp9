# Market Data Pipeline (Airflow + PostgreSQL + Superset)

This project runs a local data platform for stock ingestion, feature engineering, LSTM training/prediction, and dashboarding.

## What You Get

- Airflow DAG orchestration
- PostgreSQL storage
- pgAdmin database UI
- Superset dashboard UI

## Tech Stack and Ports

| Service | Container | Host URL / Port | Default Login |
|---|---|---|---|
| Airflow Webserver | `airflow-webserver` | `http://127.0.0.1:8080` | `admin` / `admin` |
| PostgreSQL | `airflow-postgres` | `127.0.0.1:5433` | DB: `airflow`, User: `airflow`, Pass: `airflow` |
| pgAdmin | `airflow-pgadmin` | `http://127.0.0.1:5050` | `admin@example.com` / `admin` |
| Superset | `superset` | `http://127.0.0.1:8089` | `admin` / `admin` |
| Redis | `superset-redis` | `127.0.0.1:6379` | N/A |

Notes:
- From your host machine, PostgreSQL is exposed on port `5433`.
- Inside Docker network (Airflow/Superset containers), PostgreSQL is `postgres:5432`.

## Repository Structure

```text
.
├── dags/
│   ├── backfill_pipeline.py
│   ├── market_data_pipeline.py
│   ├── lstm_prediction_pipeline.py
│   └── prediction_maintenance.py
├── scripts/
│   ├── data_validation.py
│   ├── ticker_config.py
│   ├── lstm_model.py
│   ├── superset_setup.py
│   ├── import.py
│   ├── superset_dashboard_queries.sql
│   └── superset_bootstrap.sh
├── superset/exports/
│   └── stockSight.zip
├── docker-compose.yaml
├── Dockerfile
├── Dockerfile.superset
├── requirements.txt
├── .env
├── start-docker.ps1
└── start-docker.bat
```

## Prerequisites

Install the following first:

1. Docker Desktop (Windows)
2. Git
3. Python 3.10+ (for local helper scripts)

Optional but recommended:

1. VS Code
2. PowerShell 7+

## 1) Clone and Enter Project

```powershell
git clone <YOUR_REPO_URL>
cd <YOUR_REPO_FOLDER>
```

Or if already cloned:

Go to project directory

## 2) Configure Environment File

Ensure `.env` exists in project root and contains at least:

```env
# Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=false

# App DB settings consumed in DAG code
DB_HOST=postgres
DB_USER=airflow
DB_PASSWORD=airflow
DB_NAME=airflow
DB_PORT=5432

# Postgres container
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow UI auth
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# Local Superset API base for scripts
SUPERSET_URL=http://127.0.0.1:8089
```

## 3) Start Docker Stack

From project root:

```powershell
docker compose up -d --build
```

Check service health:

```powershell
docker compose ps
```

You should see `Up` (and most services `healthy`).

## 4) Open UIs

1. Airflow: `http://127.0.0.1:8080`
2. pgAdmin: `http://127.0.0.1:5050`
3. Superset: `http://127.0.0.1:8089`

Use `127.0.0.1` (not `localhost`) to avoid Windows loopback issues.

## 5) pgAdmin Setup (One Time)

1. Open pgAdmin and log in: `admin@example.com` / `admin`.
2. Add server:
   - Name: `airflow`
   - Host: `postgres`
   - Port: `5432`
   - Maintenance DB: `airflow`
   - Username: `airflow`
   - Password: `airflow`
3. Save.

## 6) Airflow First-Run Workflow

Open Airflow UI and trigger DAGs in this order.

1. `backfill_historical_data` (manual one-off)
   - Purpose: initial historical load.

2. `market_momentum_extraction` (manual once, then scheduled `@daily`)
   - Purpose: daily ingestion + feature engineering.

3. `lstm_weekly_training` (manual once initially, then scheduled weekly)
   - Purpose: train model artifacts required for prediction.

4. `lstm_daily_prediction`
   - Triggered by dataset emitted from `market_momentum_extraction`.

5. `prediction_maintenance` (scheduled `@daily`)
   - Backfills missed predictions and updates actual closes.

## 7) Superset Database Connection (One Time)

In Superset UI:

1. Click `+` -> `Data` -> `Connect database` -> `PostgreSQL`.
2. Use:
   - Host: `postgres`
   - Port: `5432`
   - Database name: `airflow`
   - Username: `airflow`
   - Password: `airflow`
3. Test connection and Save.

## 8) Run Superset Automation Scripts

These scripts should be run from the `scripts` folder.

```powershell
cd scripts
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install requests
```

Run setup script:

```powershell
python superset_setup.py
```

Import dashboard bundle:

```powershell
python import.py
```

If you get connection reset on Windows, verify:

```powershell
$env:SUPERSET_URL
python -c "import os; print(os.getenv('SUPERSET_URL'))"
```

Expected value is:

```text
http://127.0.0.1:8089
```

## 9) Common Commands

Start stack:

```powershell
docker compose up -d
```

Rebuild stack:

```powershell
docker compose up -d --build
```

Check status:

```powershell
docker compose ps
```

Tail all logs:

```powershell
docker compose logs -f
```

Tail specific logs:

```powershell
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f superset
docker compose logs -f postgres
```

Restart one service:

```powershell
docker compose restart superset
```

Stop stack:

```powershell
docker compose down
```

Stop and remove volumes (full reset):

```powershell
docker compose down -v
```

## 10) Troubleshooting

### A) Superset script connection reset (`WinError 10054`)

1. Use `127.0.0.1` URLs, not `localhost`.
2. Confirm Superset is healthy:

```powershell
docker compose ps
```

3. Confirm env value:

```powershell
$env:SUPERSET_URL
```

4. If needed, force it for session:

```powershell
$env:SUPERSET_URL="http://127.0.0.1:8089"
python superset_setup.py
```

### B) `python superset_setup.py` says file not found

Run from `scripts` directory:

```powershell
cd scripts
python superset_setup.py
```

### C) Superset says DB `airflow` not found

You have not created Superset's DB connection yet. Complete Section 7 first.

### D) Airflow DAG imports fail

Rebuild images after dependency changes:

```powershell
docker compose up -d --build
```

## References

- Airflow docs: https://airflow.apache.org/docs/
- Docker docs: https://docs.docker.com/
- PostgreSQL docs: https://www.postgresql.org/docs/
- Superset docs: https://superset.apache.org/user-docs/