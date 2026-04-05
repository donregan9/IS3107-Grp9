# IS3107 Project Proposal
## Market Data Pipeline with Forecasting and Decision Support Dashboard

### 1. Problem Statement and Motivation

Financial market participants face a persistent challenge: market data is high volume, noisy, and time-sensitive, while actionable insights must be delivered quickly and consistently. In a student project setting, this challenge translates into an end-to-end engineering and analytics problem: how to build a reliable system that can ingest daily stock data, transform it into interpretable technical indicators, generate predictive outputs, and surface insights in a dashboard for monitoring and decision support.

This project addresses that challenge by focusing on a reproducible, automated pipeline for a representative equity ticker (currently AAPL), with infrastructure that can later scale to additional tickers. The motivation is threefold:

1. Technical integration: combine data engineering (ETL), orchestration, storage, modeling, and BI visualization in one coherent workflow.
2. Analytical value: move beyond raw OHLCV data to engineered features (e.g., RSI, MACD, Bollinger Bands) and model-driven forecasts.
3. Operational reliability: ensure outputs refresh consistently with minimal manual effort through Airflow scheduling and dependency-triggered downstream tasks.

The project is designed not as a one-off analysis notebook, but as a mini production-style analytics system. Success is defined by pipeline reliability, data freshness, model monitoring, and dashboard usability, rather than model accuracy alone.

### 2. Planned Datasets and Data Sources

#### 2.1 Primary dataset: historical and daily stock market data

- Entity: listed equity price series (current focus: AAPL).
- Core fields: date/time, open, high, low, close, volume (OHLCV).
- Ingestion cadence: scheduled daily extraction, with backfill support for historical periods.
- Storage target: PostgreSQL table stock_prices.

#### 2.2 Derived dataset: technical features

Features are computed from stock price history to represent momentum, trend, volatility, and volume behavior:

- Trend indicators: SMA(20), SMA(50), EMA(12), EMA(26).
- Momentum indicators: MACD, MACD signal, RSI(14), short-horizon returns (3-day, 5-day), daily return.
- Volatility/range indicators: Bollinger upper/lower bands, Bollinger width, rolling volatility.
- Volume indicators: volume SMA(20), volume ratio.

Storage target: PostgreSQL table stock_features.

#### 2.3 Model output dataset: forecast results

Model prediction records include:

- predicted date,
- predicted closing price,
- realized actual close when available,
- model version metadata,
- creation timestamp.

Storage target: PostgreSQL table model_predictions.

#### 2.4 Pipeline metadata

Operational metadata from Airflow (e.g., DAG run states, run durations) is used for pipeline monitoring and reliability charts.

### 3. High-Level Pipeline Design and Tools

#### 3.1 Pipeline architecture (logical flow)

1. Ingestion stage
- Fetch daily/historical market data.
- Validate schema and basic quality constraints.
- Upsert into stock_prices.

2. Feature engineering stage
- Compute indicators and returns from latest available price history.
- Write engineered feature rows to stock_features.
- Publish dataset readiness event for dependent prediction DAG.

3. Forecasting stage
- Triggered after feature dataset update (dataset-driven dependency).
- Load recent features and run LSTM inference (and training jobs on a weekly cadence).
- Persist predictions and post-hoc actuals into model_predictions.

4. BI/serving stage
- Superset connects to PostgreSQL.
- Dashboard queries consume stock_prices, stock_features, model_predictions, and Airflow metadata tables.
- Dashboard auto-refresh presents near-real-time status.

#### 3.2 Orchestration and infrastructure

- Orchestrator: Apache Airflow DAGs for extraction, feature computation, and prediction.
- Containerization: Docker Compose for reproducible local deployment.
- Database: PostgreSQL for transactional storage and queryable analytics.
- Dashboard layer: Apache Superset for charting and monitoring.
- Runtime components: Airflow webserver, scheduler, PostgreSQL, pgAdmin, Superset, Redis (for Superset support).

#### 3.3 Current DAG strategy

- Backfill DAG for historical data population.
- Market momentum/feature DAG for daily refresh.
- Dataset-based trigger from feature completion to daily prediction DAG.
- Weekly model training DAG for periodic model refresh and versioning.

This design ensures downstream prediction tasks only run when upstream feature data is ready, reducing stale-input risk.

### 4. Intended Outputs

#### 4.1 Analytical dashboard outputs

A consolidated Superset dashboard will contain at least three sections:

1. Market and indicator panel
- Price trend with moving averages (SMA/EMA).
- Candlestick and volume views.
- RSI, MACD, Bollinger bands.

2. Prediction performance panel
- Predicted vs actual close over time.
- Error trend and absolute error trend.
- MAE/RMSE by model version.
- Rolling directional accuracy.

3. Pipeline health panel
- Data freshness scorecards (latest price date, feature date, prediction date).
- DAG success/failure counts by pipeline component.
- Run duration trend for key DAGs.

#### 4.2 Modeling outputs

- Daily forecasted close value for selected ticker(s).
- Versioned model outputs for comparison across retraining cycles.
- Basic evaluation metrics to support interpretability and model governance.

#### 4.3 Engineering outputs

- Containerized stack and reproducible setup scripts.
- SQL query pack for Superset dataset creation.
- Documented workflow for onboarding teammates and rerunning pipeline.

### 5. Scope, Feasibility, and Anticipated Challenges

#### 5.1 Scope

In-scope:

- End-to-end single-ticker pipeline (AAPL) with extensibility to multi-ticker support.
- Daily data refresh and automated downstream prediction.
- Superset dashboard with both analytical and operational monitoring views.
- Baseline LSTM forecasting workflow with periodic retraining.

Out-of-scope for current phase:

- Production cloud deployment and high-availability architecture.
- Low-latency intraday trading signals.
- Portfolio optimization or execution strategies.

#### 5.2 Feasibility assessment

The project is feasible within a semester timeline because:

- Core infrastructure is already containerized.
- Pipeline DAG structure is already defined and integrated.
- Database schema for prices, features, and predictions exists.
- Dashboard integration path (Superset + PostgreSQL) is operational.

Remaining work is focused on hardening, visualization polish, and model/performance reporting rather than greenfield system design.

#### 5.3 Anticipated challenges and mitigation

1. Data quality and missing values
- Challenge: gaps in upstream market data can degrade indicator and model quality.
- Mitigation: enforce validation checks, null handling, and freshness alerting in DAG tasks.

2. Feature/model drift
- Challenge: market regimes change; fixed-parameter models can underperform.
- Mitigation: weekly retraining, versioned outputs, and dashboard error trend monitoring.

3. Pipeline dependency reliability
- Challenge: downstream tasks may run on stale or incomplete data without robust gating.
- Mitigation: dataset-triggered scheduling and explicit task dependency checks.

4. Environment and dependency issues in container stack
- Challenge: package mismatches (e.g., DB drivers) can break BI connectivity.
- Mitigation: lock setup steps in Dockerfiles/compose, verify startup health checks, and include quick diagnostics in README.

5. Interpretability for non-technical stakeholders
- Challenge: model outputs alone are hard to trust.
- Mitigation: pair predictions with technical indicators, error metrics, and pipeline health charts in a single dashboard narrative.

### 6. Expected Deliverable Quality Criteria

The final system will be considered successful if it demonstrates:

- Automated daily refresh from ingestion to dashboard visibility.
- Consistent prediction generation with versioned tracking.
- Dashboard clarity for both market interpretation and system observability.
- Reproducible setup on another team member machine with minimal manual intervention.

### 7. Conclusion

This project delivers a realistic, full-stack data product for market analytics: from data ingestion and feature engineering to forecasting and dashboard-based decision support. It is intentionally scoped to balance technical depth with implementation feasibility. By combining orchestration, modeling, and visualization in one pipeline, the project provides both practical engineering value and analytical insight, while leaving a clear path for future extension to multi-asset and cloud-native deployments.
