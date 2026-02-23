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
