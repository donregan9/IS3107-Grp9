# IS3107 Milestone D: Data Quality, Incremental Dedup, and Validation Evidence

## 1. Purpose

This document consolidates the production-grade controls added to the market data pipeline and provides a repeatable verification checklist for demonstration and reporting.

Scope of this milestone:
- Document final data validation architecture
- Document incremental ingestion and deduplication strategy
- Provide validation and audit evidence queries
- Provide test script/checklist for reproducible proof

---

## 2. Pipeline Context

Primary implementation file:
- dags/market_data_pipeline.py

Daily flow:
1. create_table
2. fetch_daily_stock_data (currently mapped to smart_fetch_and_store)
3. compute_features

Core data stores:
- stock_prices: raw validated OHLCV data
- stock_features: engineered features for model input
- data_quality_results: check-by-check quality audit log

---

## 3. Incremental Update and Deduplication Strategy

### 3.1 Incremental ingestion logic

The ingestion task first reads MAX(date) from stock_prices for a ticker, then only processes API rows with date > last_date.

Benefits:
- Avoids unnecessary reprocessing of already-ingested rows
- Handles Docker downtime catch-up (multiple missing days)

### 3.2 Deduplication and idempotency

Deduplication is enforced at database level:
- UNIQUE(ticker, date) on stock_prices

Writes use UPSERT:
- INSERT ... ON CONFLICT (ticker, date) DO UPDATE

Result:
- Re-runs are idempotent
- Overlapping records do not create duplicates
- Corrected data from source can overwrite stale values safely

---

## 4. Data Validation Framework

Validation is layered across ingestion and feature engineering.

### 4.1 API-level validation

Checks:
- HTTP request success (with timeout)
- Valid JSON payload
- API error messages
- API rate-limit/notice responses
- Presence of Time Series (Daily)

Failure behavior:
- Hard fail task when API payload integrity is not acceptable

### 4.2 Row-level raw OHLCV validation

For each candidate row:
- Required fields exist
- Date format is valid (YYYY-MM-DD)
- Numeric conversion succeeds
- Domain constraints:
  - open/high/low/close > 0
  - volume >= 0
  - high >= low
  - open within [low, high]
  - close within [low, high]

Batch controls:
- Reject counter by reason
- Hard fail if all candidate rows are rejected
- Hard fail if rejected ratio exceeds MAX_REJECT_RATIO

### 4.3 Freshness and completeness checks

Checks:
- API freshness: staleness days <= MAX_API_STALENESS_DAYS
- Incremental completeness over business days:
  - completeness_ratio = available_market_days / expected_market_days
  - must satisfy completeness_ratio >= MIN_INCREMENTAL_COMPLETENESS

Failure behavior:
- Hard fail on stale source data
- Hard fail on severe missing-day gaps

### 4.4 Feature-level validation

Checks before upsert:
- Source base null ratio for close/volume <= MAX_SOURCE_NULL_RATIO
- Feature DataFrame not empty after dropna
- No null cells in required feature columns
- RSI range within [0, 100]
- volatility_14 non-negative

Failure behavior:
- Hard fail on feature integrity violations

---

## 5. Quality Audit and Evidence

All major checks write records into data_quality_results:
- dag_id, task_id, run_id
- ticker
- check_name, status
- observed_value, threshold
- failed_count
- details (JSON text)
- created_at

This supports:
- Traceability per DAG run
- Defensible evidence for tutor/instructor review
- Faster post-mortem debugging

---

## 6. Verification Runbook (Step-by-Step)

### Step 1: Run one DAG cycle

Trigger DAG:
- market_momentum_extraction

Expected:
- create_table success
- fetch_daily_stock_data success (or no new data)
- compute_features success

### Step 2: Confirm quality checks were logged

Run:

```sql
SELECT created_at, dag_id, task_id, run_id, ticker,
       check_name, status, observed_value, threshold, failed_count
FROM data_quality_results
ORDER BY created_at DESC
LIMIT 50;
```

Expected checks in recent rows:
- api_payload_valid
- api_freshness
- incremental_completeness
- row_validation
- reject_ratio_threshold (only if failure condition reached)
- rows_upserted
- source_base_null_ratio
- feature_rows_after_dropna
- feature_null_cells
- rsi_range_check
- volatility_non_negative
- feature_rows_upserted

### Step 3: Validate deduplication behavior

Re-run the same DAG with no meaningful new market day.

Run:

```sql
SELECT ticker, date, COUNT(*)
FROM stock_prices
GROUP BY ticker, date
HAVING COUNT(*) > 1;
```

Expected:
- Zero rows returned

### Step 4: Validate incremental behavior

Run:

```sql
SELECT ticker, MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS rows
FROM stock_prices
GROUP BY ticker;
```

Expected:
- max_date should advance only when new market data exists
- No abrupt duplication spikes

### Step 5: Validate feature data integrity

Run:

```sql
SELECT
  SUM(CASE WHEN rsi_14 < 0 OR rsi_14 > 100 THEN 1 ELSE 0 END) AS rsi_out_of_range,
  SUM(CASE WHEN volatility_14 < 0 THEN 1 ELSE 0 END) AS negative_volatility,
  SUM(CASE WHEN close IS NULL OR daily_return IS NULL OR sma_20 IS NULL OR sma_50 IS NULL
           OR ema_12 IS NULL OR ema_26 IS NULL OR macd IS NULL OR macd_signal IS NULL
           OR rsi_14 IS NULL OR bb_upper IS NULL OR bb_lower IS NULL
           OR volatility_14 IS NULL OR volume_sma_20 IS NULL OR bb_width IS NULL
           OR volume_ratio_20 IS NULL OR return_3d IS NULL OR return_5d IS NULL
      THEN 1 ELSE 0 END) AS null_feature_rows
FROM stock_features
WHERE ticker = 'AAPL';
```

Expected:
- All three values should be 0

---

## 7. Suggested Demo Narrative (for tutor)

1. Explain that ingestion is incremental using MAX(date), not full refresh.
2. Show UNIQUE + UPSERT for strict dedup and idempotency.
3. Show audit records in data_quality_results for one successful run.
4. Show a forced failure case (e.g., invalid API key) and corresponding fail audit row.
5. Show feature quality checks and zero-violation SQL outputs.

---

## 8. Current Configuration Thresholds

From market_data_pipeline.py:
- MAX_REJECT_RATIO = 0.20
- MAX_API_STALENESS_DAYS = 5
- MIN_INCREMENTAL_COMPLETENESS = 0.90
- MAX_SOURCE_NULL_RATIO = 0.01

These thresholds are conservative defaults and can be tuned after observing live behavior over several weeks.

---

## 9. Limitations and Next Improvements

Current limitations:
- Completeness uses business-day approximation, not exchange holiday calendar.
- Data quality checks are currently hard-fail for major anomalies, which may be strict during API instability.

Recommended next improvements:
1. Use exchange calendar library for trading-day-aware completeness.
2. Add severity-based policy (warn vs fail) per check type.
3. Add dashboarding in Superset for data_quality_results trends.
4. Add automated alerts on repeated failures.

---

## 10. Conclusion

The pipeline now has:
- Explicit, layered validation controls
- Robust incremental ingestion with deduplication
- Auditable quality evidence per DAG run

This satisfies the tutor's feedback on data validation and clarifies the technical strategy for incremental updates versus full refresh.
