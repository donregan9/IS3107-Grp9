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

# SuperSet Dashboard Setup guide

This setup will be using docker compose.

1. Clone Superset Repo

git clone https://github.com/apache/superset

2. Start Up Superset in terminal

cd superset
git checkout tags/6.0.0
docker compose -f docker-compose-image-tag.yml up

3. Navigate to GUI in localhost (default is 8088)

http://localhost:8088/
Default credentials: 
username: admin
password: admin

To create new users for testing, run in terminal:
docker exec -it superset_app superset fab create-admin

# Superset user guide

There are numerouse pre-generated superset dashboards to explore.

To start up a SHARED dashboard (via Import):

1. Navigate to Dashboard tab
2. Click Box-Arrow Icon (Import) in top-right conner next to Bulk Select and +Dashboard
3. Select the shared zip folder of the dashboard

`For now we can try sharing dashboards via telegram`

To Export Dashboard for Sharing:
1. Dashboard Tab
2. Click Export next to the Dashboard
3. Installed the zip folder

# Superset Connect DB Guide

1. Top-right corner `+` sign
2. Connect to Database, choose PostgresSQL
3. Enter the following details for the DB

(If you configured different password in PGAdmin, input accordingly)

Host: host.docker.internal (this is docker local for windows, may be diff for macOS)

Port: 5432

name: airflow

password: airflow

Display Name: IS3107PostgreSQL (Or any other name)

4. Try connecting, if successful, navigate to SQL Lab (Don't need insert new data)

Test with (should return same results):

SELECT * FROM public.stock_prices
ORDER BY id ASC 

5. Save that query as a dataset (choose a name)
6. Now able to create charts using that dataset

Note: While it pulls data from the DB, it will only pull whatever has been run already;
   Need to rerun the DAG to pull daily results if you did not leave container running overnight, 
   else there will not be the most recent data.


# To remove Superset
1. Remove like any normal container environment

docker compose down
