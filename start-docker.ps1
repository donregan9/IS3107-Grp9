#!/usr/bin/env pwsh

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Market Data Pipeline Docker Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is installed
try {
    $dockerVersion = docker --version
    Write-Host "[OK] Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Docker is not installed or not in PATH" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "[INFO] Starting Docker services..." -ForegroundColor Yellow

# Start services
docker compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "Docker services started successfully!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Airflow UI:  http://localhost:8080" -ForegroundColor Cyan
    Write-Host "pgAdmin:     http://localhost:5050" -ForegroundColor Cyan
    Write-Host "PostgreSQL:  localhost:5432" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Default credentials:" -ForegroundColor Yellow
    Write-Host "  Airflow: admin / admin"
    Write-Host "  pgAdmin: admin@example.com / admin"
    Write-Host ""
    Write-Host "Useful commands:" -ForegroundColor Yellow
    Write-Host "  Check status: docker compose ps"
    Write-Host "  View logs:    docker compose logs -f"
    Write-Host "  Stop:         docker compose down"
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[ERROR] Failed to start services" -ForegroundColor Red
    Write-Host "Please check the error messages above" -ForegroundColor Red
    Write-Host ""
    exit 1
}
