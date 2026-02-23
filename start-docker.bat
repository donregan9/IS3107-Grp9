@echo off
REM Market Data Pipeline Docker Launcher

echo.
echo ========================================
echo Market Data Pipeline Docker Setup
echo ========================================
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH
    echo.
    echo Please install Docker Desktop from: https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

echo [OK] Docker found
docker --version
echo.

echo [INFO] Starting Docker services...
docker compose up -d

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo Docker services started successfully!
    echo ========================================
    echo.
    echo Airflow UI:  http://localhost:8080
    echo pgAdmin:     http://localhost:5050
    echo PostgreSQL:  localhost:5432
    echo.
    echo Default credentials:
    echo   Airflow: admin / admin
    echo   pgAdmin: admin@example.com / admin
    echo.
    echo To check status: docker compose ps
    echo To view logs: docker compose logs -f
    echo.
    pause
) else (
    echo.
    echo [ERROR] Failed to start services
    echo Please check the error messages above
    echo.
    pause
    exit /b 1
)
