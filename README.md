# Market Data Pipeline - Docker Setup

This project sets up Apache Airflow with PostgreSQL in Docker for automated market data extraction and storage.

## Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   └── market_data_pipeline.py
├── scripts/                   # Custom extraction and processing scripts
│   └── extract.py
├── data/                      # Local data storage (CSVs, backups)
├── logs/                      # Airflow logs
├── docker-compose.yaml        # Docker services definition
├── Dockerfile                 # Custom Airflow image with dependencies
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables
└── README.md                  # This file
```

## Services

1. **PostgreSQL** (port 5432)
   - Stores stock prices and metadata
   - User: `airflow` / Password: `airflow`
   - Database: `airflow`

2. **Apache Airflow** (port 8080)
   - Webserver: UI for managing DAGs
   - Scheduler: Automatically runs tasks on schedule
   - User: `admin` / Password: `admin`

3. **pgAdmin** (port 5050)
   - Web interface to manage PostgreSQL
   - Email: `admin@example.com` / Password: `admin`

## Quick Start

### Prerequisites
- Docker Desktop installed
- Docker Compose installed
- On Windows, use PowerShell or WSL2

### Launch the Stack

```powershell
# Navigate to project directory
cd "C:\Users\regan\OneDrive\Desktop\Uni\Y3S2\IS3107\Project"

# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

### Access the UIs

- **Airflow**: http://localhost:8080 (admin/admin)
- **pgAdmin**: http://localhost:5050 (admin@example.com/admin)
- **PostgreSQL**: localhost:5432

### View Logs

```powershell
# Watch scheduler logs
docker-compose logs -f airflow-scheduler

# Watch webserver logs
docker-compose logs -f airflow-webserver

# View all logs
docker-compose logs -f
```

### Stop the Stack

```powershell
# Stop all containers
docker-compose down

# Stop and remove volumes (clears database)
docker-compose down -v
```

## Configuration

Edit `.env` file to change:
- Database credentials
- Airflow admin credentials
- Airflow configuration variables

## Adding New DAGs

1. Create a new Python file in `/dags/` folder
2. Define your DAG using Airflow syntax
3. Restart Airflow webserver (or it will auto-detect within ~5 minutes)

## DAG Orchestration (Market -> Prediction)

This project uses Airflow Datasets so prediction runs only after features are refreshed.

How it works:
1. market_momentum_extraction runs create_table -> fetch_daily_stock_data -> compute_features.
2. compute_features publishes dataset://stock_features/aapl/ready on success.
3. lstm_daily_prediction is scheduled on that dataset and starts after the dataset event is emitted.

Trigger behavior:
1. Daily scheduled market run success triggers prediction.
2. Manual market run success also triggers prediction.
3. If compute_features fails, no dataset event is emitted, so prediction will not run.

Notes:
1. Dataset scheduling requires Airflow 2.4 or later.
2. After DAG code changes, wait for scheduler refresh or restart airflow-scheduler and airflow-webserver.

## Adding Dependencies

1. Update `requirements.txt`
2. Rebuild the image: `docker-compose build`
3. Restart services: `docker-compose up -d`

## Troubleshooting

### Airflow won't start
```powershell
# Check logs
docker-compose logs airflow-webserver

# Restart services
docker-compose restart airflow-webserver airflow-scheduler
```

### PostgreSQL connection fails
```powershell
# Check if postgres is healthy
docker-compose ps

# Restart postgres
docker-compose restart postgres
```

### Port already in use
Edit `docker-compose.yaml` and change ports (e.g., 8081:8080 for Airflow)

## Set-up Process

Navigate to postgresql
1. Add New Server
2. Server Name: airflow
3. Host: postgres
4. Port: 5432
5. Database, Username and password: airflow

Navigate to airflow
1. Run Backfill DAG
2. Run Market Momentum DAG
3. Run Weekly training
4. Run Daily prediction

Follow the steps on Superset Setup Guide Below

## Next Steps

1. Modify `market_data_pipeline.py` DAG to fetch different tickers
2. Extend the extraction logic in `/scripts/extract.py`
3. Add data transformation tasks to the DAG
4. Set up alerts and monitoring
5. Deploy to GCP Cloud Composer for production

## Support

For issues with:
- **Airflow**: https://airflow.apache.org/docs/
- **Docker**: https://docs.docker.com/
- **PostgreSQL**: https://www.postgresql.org/docs/

# Superset Dashboard Setup Guide

Superset is already included in this project's docker-compose stack.

1. Start services:

docker compose up -d

2. Open Superset:

http://localhost:8089

Default credentials:
- username: admin
- password: admin

# Team-Shared Dashboards (Dashboards as Code)

This repo is configured to auto-import dashboard export bundles on Superset startup.

Import folder in git:
- superset/exports/

How to share dashboards with teammates:
1. In Superset, export your dashboard bundle as a .zip.
2. Put the zip file into superset/exports/.
3. Commit and push.
4. Teammates pull and run docker compose up -d (or docker compose restart superset).
5. Superset imports bundles automatically at startup.

Notes:
1. Import uses overwrite mode where supported, so updates can be rolled out by replacing the zip.
2. Keep only the dashboard bundles you want auto-loaded in superset/exports/.

# Superset Connect DB Guide

1. Click + (top-right) -> Connect Database -> PostgreSQL.
2. Use:

Host: postgres
Port: 5432
Database: airflow
Username: airflow
Password: airflow

3. Test connection, then create datasets/charts.

Sample query:

SELECT * FROM public.stock_prices
ORDER BY id ASC;

# To Remove Superset

docker compose down
