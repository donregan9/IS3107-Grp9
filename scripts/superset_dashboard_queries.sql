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

-- 13) Latest price date (Live Status card)
SELECT
  ticker,
  MAX(date) AS latest_price_date
FROM stock_prices
GROUP BY ticker
ORDER BY ticker;

-- 14) Latest feature date (Live Status card)
SELECT
  ticker,
  MAX(date) AS latest_feature_date
FROM stock_features
GROUP BY ticker
ORDER BY ticker;

-- 15) Latest prediction date (Live Status card)
SELECT
  ticker,
  model_version,
  MAX(predicted_date) AS latest_prediction_date
FROM model_predictions
GROUP BY ticker, model_version
ORDER BY ticker, model_version;