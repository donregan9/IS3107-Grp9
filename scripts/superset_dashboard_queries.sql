-- Superset dashboard query pack for IS3107 project
-- Data tables used: stock_prices, stock_features, model_predictions, dag_run
-- Default ticker in examples: AAPL

-- 1) Price trend with moving averages (Line chart)
SELECT
  date AS ds,
  close,
  sma_20,
  sma_50,
  ema_12,
  ema_26
FROM stock_features
WHERE ticker = 'AAPL'
ORDER BY ds;

-- 2) Candlestick source (ECharts Candlestick)
SELECT
  date AS ds,
  open,
  high,
  low,
  close,
  volume
FROM stock_prices
WHERE ticker = 'AAPL'
ORDER BY ds;

-- 3) RSI momentum (Line chart)
SELECT
  date AS ds,
  rsi_14
FROM stock_features
WHERE ticker = 'AAPL'
  AND rsi_14 IS NOT NULL
ORDER BY ds;

-- 4) MACD vs signal (Dual line chart)
SELECT
  date AS ds,
  macd,
  macd_signal
FROM stock_features
WHERE ticker = 'AAPL'
  AND macd IS NOT NULL
  AND macd_signal IS NOT NULL
ORDER BY ds;

-- 5) Bollinger bands (Line chart)
SELECT
  date AS ds,
  close,
  bb_upper,
  bb_lower
FROM stock_features
WHERE ticker = 'AAPL'
  AND bb_upper IS NOT NULL
  AND bb_lower IS NOT NULL
ORDER BY ds;

-- 6) Model prediction vs actual (Line chart)
SELECT
  predicted_date AS ds,
  predicted_close,
  actual_close,
  (predicted_close - actual_close) AS error,
  ABS(predicted_close - actual_close) AS abs_error
FROM model_predictions
WHERE ticker = 'AAPL'
ORDER BY ds;

-- 7) Prediction quality by model version (Bar chart)
SELECT
  model_version,
  COUNT(*) AS n_predictions,
  AVG(ABS(predicted_close - actual_close)) AS mae,
  SQRT(AVG(POWER(predicted_close - actual_close, 2))) AS rmse
FROM model_predictions
WHERE ticker = 'AAPL'
  AND actual_close IS NOT NULL
GROUP BY model_version
ORDER BY model_version;

-- 8) Directional accuracy over time (Line or area chart)
WITH base AS (
  SELECT
    predicted_date,
    predicted_close,
    actual_close,
    LAG(actual_close) OVER (PARTITION BY ticker ORDER BY predicted_date) AS prev_actual_close
  FROM model_predictions
  WHERE ticker = 'AAPL'
),
flags AS (
  SELECT
    predicted_date AS ds,
    CASE
      WHEN actual_close IS NULL OR prev_actual_close IS NULL THEN NULL
      WHEN predicted_close >= prev_actual_close AND actual_close >= prev_actual_close THEN 1.0
      WHEN predicted_close < prev_actual_close AND actual_close < prev_actual_close THEN 1.0
      ELSE 0.0
    END AS direction_hit
  FROM base
)
SELECT
  ds,
  AVG(direction_hit) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_directional_accuracy
FROM flags
ORDER BY ds;

-- 9) Daily return distribution (Histogram)
SELECT
  daily_return
FROM stock_features
WHERE ticker = 'AAPL'
  AND daily_return IS NOT NULL;

-- 10) Pipeline freshness scorecard (Big number / table)
SELECT
  (SELECT MAX(date) FROM stock_prices WHERE ticker = 'AAPL') AS latest_price_date,
  (SELECT MAX(date) FROM stock_features WHERE ticker = 'AAPL') AS latest_feature_date,
  (SELECT MAX(predicted_date) FROM model_predictions WHERE ticker = 'AAPL') AS latest_prediction_date,
  NOW() AS checked_at;

-- 11) DAG reliability (Bar chart by DAG and state)
SELECT
  dag_id,
  state,
  COUNT(*) AS run_count
FROM dag_run
WHERE dag_id IN ('market_momentum_extraction', 'lstm_daily_prediction', 'lstm_weekly_training')
GROUP BY dag_id, state
ORDER BY dag_id, state;

-- 12) DAG run duration trend (Line chart)
SELECT
  dag_id,
  execution_date::date AS run_date,
  EXTRACT(EPOCH FROM (end_date - start_date)) / 60.0 AS duration_minutes
FROM dag_run
WHERE dag_id IN ('market_momentum_extraction', 'lstm_daily_prediction', 'lstm_weekly_training')
  AND state = 'success'
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
ORDER BY run_date;


-- -------------------------------------------------------------------------
-- Dashboard Build Pack (aligned to presentation layout)
-- Includes prediction_horizon for dashboard filter controls.
-- prediction_horizon: daily (model_predictions) or backfill (backfill_model_predictions)
-- -------------------------------------------------------------------------

-- 13) Header card: latest price date
SELECT MAX(date)::date AS latest_price_date
FROM stock_prices
WHERE ticker = 'AAPL';

-- 14) Header card: latest feature date
SELECT MAX(date)::date AS latest_feature_date
FROM stock_features
WHERE ticker = 'AAPL';

-- 15) Header card: latest prediction date (across daily + backfill)
WITH prediction_base AS (
  SELECT ticker, predicted_date, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
)
SELECT MAX(predicted_date)::date AS latest_prediction_date
FROM prediction_base
WHERE ticker = 'AAPL';

-- 16) Header card: 7-day MAE (latest 7 comparable predictions)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
), ranked AS (
  SELECT
    ticker,
    predicted_date,
    ABS(predicted_close - actual_close) AS abs_error,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY predicted_date DESC) AS rn
  FROM prediction_base
  WHERE ticker = 'AAPL'
    AND actual_close IS NOT NULL
)
SELECT AVG(abs_error) AS mae_7d
FROM ranked
WHERE rn <= 7;

-- 17) Header card: 7-day directional accuracy (latest 7 comparable predictions)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
), base AS (
  SELECT
    ticker,
    predicted_date,
    predicted_close,
    actual_close,
    LAG(actual_close) OVER (PARTITION BY ticker ORDER BY predicted_date) AS prev_actual_close
  FROM prediction_base
  WHERE ticker = 'AAPL'
), flags AS (
  SELECT
    predicted_date,
    CASE
      WHEN actual_close IS NULL OR prev_actual_close IS NULL THEN NULL
      WHEN predicted_close >= prev_actual_close AND actual_close >= prev_actual_close THEN 1.0
      WHEN predicted_close < prev_actual_close AND actual_close < prev_actual_close THEN 1.0
      ELSE 0.0
    END AS direction_hit
  FROM base
), ranked AS (
  SELECT
    predicted_date,
    direction_hit,
    ROW_NUMBER() OVER (ORDER BY predicted_date DESC) AS rn
  FROM flags
  WHERE direction_hit IS NOT NULL
)
SELECT AVG(direction_hit) AS directional_accuracy_7d
FROM ranked
WHERE rn <= 7;

-- 18) Model performance: predicted vs actual (supports horizon filter)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
)
SELECT
  predicted_date AS ds,
  ticker,
  model_version,
  prediction_horizon,
  predicted_close,
  actual_close
FROM prediction_base
WHERE ticker = 'AAPL'
ORDER BY ds;

-- 19) Model performance: error over time (supports horizon filter)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
)
SELECT
  predicted_date AS ds,
  ticker,
  model_version,
  prediction_horizon,
  (predicted_close - actual_close) AS error,
  ABS(predicted_close - actual_close) AS abs_error
FROM prediction_base
WHERE ticker = 'AAPL'
  AND actual_close IS NOT NULL
ORDER BY ds;

-- 20) Stability: rolling MAE and RMSE (7-day, supports horizon filter)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
), e AS (
  SELECT
    predicted_date,
    ticker,
    model_version,
    prediction_horizon,
    ABS(predicted_close - actual_close) AS abs_error,
    POWER(predicted_close - actual_close, 2) AS sq_error
  FROM prediction_base
  WHERE ticker = 'AAPL'
    AND actual_close IS NOT NULL
)
SELECT
  predicted_date AS ds,
  ticker,
  model_version,
  prediction_horizon,
  AVG(abs_error) OVER (
    PARTITION BY ticker, model_version, prediction_horizon
    ORDER BY predicted_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_mae_7d,
  SQRT(AVG(sq_error) OVER (
    PARTITION BY ticker, model_version, prediction_horizon
    ORDER BY predicted_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )) AS rolling_rmse_7d
FROM e
ORDER BY ds;

-- 21) Stability: rolling directional accuracy (7-day, supports horizon filter)
WITH prediction_base AS (
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'daily'::text AS prediction_horizon
  FROM model_predictions
  UNION ALL
  SELECT ticker, predicted_date, predicted_close, actual_close, model_version, 'backfill'::text AS prediction_horizon
  FROM backfill_model_predictions
), base AS (
  SELECT
    ticker,
    model_version,
    prediction_horizon,
    predicted_date,
    predicted_close,
    actual_close,
    LAG(actual_close) OVER (
      PARTITION BY ticker, model_version, prediction_horizon
      ORDER BY predicted_date
    ) AS prev_actual_close
  FROM prediction_base
  WHERE ticker = 'AAPL'
), flags AS (
  SELECT
    ticker,
    model_version,
    prediction_horizon,
    predicted_date,
    CASE
      WHEN actual_close IS NULL OR prev_actual_close IS NULL THEN NULL
      WHEN predicted_close >= prev_actual_close AND actual_close >= prev_actual_close THEN 1.0
      WHEN predicted_close < prev_actual_close AND actual_close < prev_actual_close THEN 1.0
      ELSE 0.0
    END AS direction_hit
  FROM base
)
SELECT
  predicted_date AS ds,
  ticker,
  model_version,
  prediction_horizon,
  AVG(direction_hit) OVER (
    PARTITION BY ticker, model_version, prediction_horizon
    ORDER BY predicted_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_directional_accuracy
FROM flags
ORDER BY ds;

-- 22) Regime/explainability: Bollinger width and volatility trend
SELECT
  date AS ds,
  ticker,
  bb_width,
  volatility_14
FROM stock_features
WHERE ticker = 'AAPL'
  AND bb_width IS NOT NULL
  AND volatility_14 IS NOT NULL
ORDER BY ds;

-- 23) Prediction Residuals (Predicted - Actual)
SELECT 
    predicted_date AS ds,
    ticker,
    model_version,
    (predicted_close - actual_close) AS residual_error
FROM model_predictions
WHERE actual_close IS NOT NULL
ORDER BY ds ASC;

-- 24) Prediction Error Distribution (Histogram)
SELECT 
    (predicted_close - actual_close) AS prediction_error
FROM model_predictions
WHERE actual_close IS NOT NULL;
