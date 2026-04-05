# IS3107 Project Architecture Blueprint

## Architecture Pattern

Pipeline style: Layered batch analytics pipeline with dataset-triggered inference.

Core idea:
- Use scheduled and manual batch ingestion for market data.
- Transform raw OHLCV into technical indicators.
- Trigger prediction only after fresh feature data is ready.
- Serve both analytical and operational insights through Superset.

---

## 1. Data Ingestion Layer

### Goal
Collect reliable historical and daily stock market data for downstream analytics and modeling.

### Inputs
- EOD historical/daily price series (OHLCV) for target ticker(s), currently AAPL.
- Optional external news/sentiment or macro signals (future extension).

### Processing
- Airflow DAG fetches data from source APIs.
- Basic schema checks and dedup/upsert logic.
- Backfill DAG supports historical population.

### Output Table
- public.stock_prices

### Key Fields
- ticker, date, open, high, low, close, volume, created_at, updated_at

---

## 2. Storage and Data Model Layer

### Primary Store
- PostgreSQL (single system of record for pipeline + dashboard)

### Core Tables
- public.stock_prices: raw market prices
- public.stock_features: engineered indicators and returns
- public.model_predictions: forecast outputs and model versioning
- public.dag_run: orchestration metadata for reliability monitoring

### Design Approach
- Star-like analytical shape for dashboard queries:
  - fact_prices (stock_prices)
  - fact_features (stock_features)
  - fact_predictions (model_predictions)
  - dim_time can be derived from date fields when needed

---

## 3. Transformation Layer

### Goal
Convert raw prices into model-ready and dashboard-ready technical features.

### Feature Engineering
- Trend: sma_20, sma_50, ema_12, ema_26
- Momentum: macd, macd_signal, rsi_14, return_3d, return_5d, daily_return
- Volatility/range: bb_upper, bb_lower, bb_width, volatility_14
- Volume behavior: volume_sma_20, volume_ratio_20

### Processing Behavior
- Airflow task computes rolling-window metrics.
- Writes latest engineered records to public.stock_features.
- Emits dataset readiness event for downstream prediction DAG.

---

## 4. ML / Forecasting Layer

### Goal
Generate next-step closing price forecasts and track model quality over time.

### Model Workflow
- Daily inference DAG runs when feature dataset is refreshed.
- Weekly training DAG updates model version periodically.
- Predictions written to public.model_predictions.

### Stored Outputs
- ticker, predicted_date, predicted_close, actual_close, model_version, created_at

### Monitoring Metrics
- MAE, RMSE by model_version
- Prediction error trend and directional hit-rate

---

## 5. Orchestration Layer

### Tool
- Apache Airflow (webserver + scheduler + DAG metadata in PostgreSQL)

### DAG Topology
- backfill_historical_data
- market_momentum_extraction
- lstm_daily_prediction (dataset-triggered by refreshed features)
- lstm_weekly_training

### Trigger Strategy
- Scheduled runs for routine refresh.
- Manual trigger for testing/recovery.
- Dataset-based dependency to avoid stale prediction inputs.

---

## 6. Serving and Dashboard Layer

### Tool
- Apache Superset

### Dashboard Modules
1. Market behavior
- Candlestick + volume
- Close price with SMA/EMA trend lines
- RSI, MACD, Bollinger bands

2. Model performance
- Predicted vs actual close
- Error and absolute error over time
- MAE/RMSE by model version
- Rolling directional accuracy

3. Pipeline operations
- DAG success/failure counts
- DAG duration trend
- Data freshness cards (latest price/feature/prediction timestamps)

### Consumption Pattern
- Superset queries PostgreSQL directly.
- Dashboard refresh interval keeps visual state near real-time.

---

## 7. Technology Stack

- Orchestration: Apache Airflow
- Compute/ETL: Python
- Storage: PostgreSQL
- Visualization: Apache Superset
- Containerization: Docker + Docker Compose
- Modeling: LSTM-based forecasting pipeline
- Dev Ops support: pgAdmin, container health checks

---

## 8. Scope, Feasibility, and Risks

### Current Scope (In)
- End-to-end automated pipeline for AAPL.
- Feature generation, prediction, and dashboard integration.
- Monitoring of both model outputs and pipeline reliability.

### Future Scope (Out for now)
- Multi-ticker scale-out and portfolio analytics.
- Cloud-native deployment (managed Airflow/DB/BI services).
- Intraday low-latency forecasting.

### Main Risks and Mitigation
- Data gaps or API drift:
  - Add validation, null handling, and fallback logging.
- Model performance drift:
  - Weekly retraining and version-aware performance charts.
- Pipeline breakages:
  - Dataset-trigger gating, retry policies, and DAG-level monitoring.
- Dependency/config mismatch in containers:
  - Pin images/dependencies and keep reproducible compose setup.

---

## 9. One-View Logical Flow

1. Source API -> 2. stock_prices -> 3. stock_features -> 4. model_predictions -> 5. Superset Dashboard

Airflow coordinates all transitions, and DAG metadata feeds operational charts for reliability visibility.
