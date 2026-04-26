# Market Data Pipeline - Docker Setup

This project sets up Apache Airflow with PostgreSQL in Docker for automated market data extraction and storage.

## Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   └── backfill_pipeline.py
│   └── lstm_prediction_pipeline.py
│   └── market_data_pipeline.py
│   └── prediction_maintenance.py
├── scripts/                   # Custom extraction and processing scripts
│   └── data_validation.py
│   └── import.py
│   └── lstm_model.py
│   └── superset_bootsrap.sh
│   └── superset_dashboard_queries.sql
│   └── superset_setup.py
│   └── ticker_config.py
├── superset\exports           # Completed Dashboard
│   └── stockSight.zip
├── logs/                      # Airflow logs
├── docker-compose.yaml        # Docker services definition
├── Dockerfile                 # Custom Airflow image with dependencies
├── Dockerfile.superset        # Custom Airflow image for superset
├── requirements.txt           
├── .env                  
├── start-docker.bat    
├── start-docker.ps1         
└── README.md                  
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

4. **superset** (port 8089)
   - Web interface to manage PostgreSQL
   - Email: `admin` / Password: `admin`

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
- **superset**: http://localhost:8089 (admin/admin)
- **PostgreSQL**: localhost:5432

## Project Setup Guide

1. Launch the stack: docker-compose up -d

2. Navigate to pgadmin: http://localhost:5050 
- `admin@example.com`
3. Create new database `airflow`
- server name: `airflow`
- host: `postgres`
- port: `5432`
- database, username & password: `airflow`

4. Navigate to airflow: http://localhost:5050 
5. Run backfill_historical_data dag
6. Run lstm_weekly_training
7. Run market_momentum_extraction

8. Navigate to superset: http://localhost:8089 
9. Connect new postgreSQL database
- Host: `postgres`
- Port: `5432`
- Database name: `airflow`
- Database password: `airflow`
10. Run superset_setup.py
11. Run import.py

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

## DAG Orchestration (Market -> Prediction)

backfill_historical_data:
1. runs once to populate database with historical information

backfill_historical_data:
1. runs weekly to retrain the model and produce a new version best suited to current trading patterns
2. must be run before lstm_daily_prediction

market_momentum_extraction:
1. market_momentum_extraction runs daily to create_table -> fetch_daily_stock_data -> compute_features.
2. compute_features publishes dataset://stock_features/aapl/ready on success.
3. lstm_daily_prediction is scheduled on that dataset and starts after the dataset event is emitted.

lstm_daily_prediction:
1. runs automatically daily after market_momentum_extraction is completed

prediction_maintenance:
1. runs daily to fill up missed values during container downtime


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

## Support

For issues with:
- **Airflow**: https://airflow.apache.org/docs/
- **Docker**: https://docs.docker.com/
- **PostgreSQL**: https://www.postgresql.org/docs/
- **Superset**: https://superset.apache.org/user-docs/