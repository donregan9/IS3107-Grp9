-- Superset dashboard query pack for IS3107 project
-- Data tables used: stock_prices, stock_features, model_predictions, dag_run

-- 1) Model prediction vs actual (Line chart)
SELECT
  predicted_date AS ds,
  ticker,
  predicted_close,
  actual_close,
  (predicted_close - actual_close) AS error,
  ABS(predicted_close - actual_close) AS abs_error
FROM model_predictions
ORDER BY ds;

-- 2) Directional accuracy over time (Line or area chart)
WITH base AS (
  SELECT
    ticker,
    predicted_date,
    predicted_close,
    actual_close,
    LAG(actual_close) OVER (PARTITION BY ticker ORDER BY predicted_date) AS prev_actual_close
  FROM model_predictions
),
flags AS (
  SELECT
    ticker,
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
  ticker,
  AVG(direction_hit) OVER (PARTITION BY ticker ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_directional_accuracy
FROM flags
ORDER BY ds;


-- 3) DAG reliability (Bar chart by DAG and state)
-- NOTE: No ticker here, exclude this chart from the filter in Superset UI Scoping
SELECT
  dag_id,
  state,
  COUNT(*) AS run_count
FROM dag_run
WHERE dag_id IN (
  'market_momentum_extraction',
  'lstm_daily_prediction',
  'lstm_weekly_training',
  'backfill_historical_data',
  'backfill_predictions',
  'missed_predictions'
)
GROUP BY dag_id, state
ORDER BY dag_id, state;

-- 4) Latest Price Date (Live Status card)
SELECT
  ticker,                     -- Invisible hook for the Native Filter
  ticker AS "Stock Ticker",   -- Pretty name for the frontend table
  MAX(date) AS "Last Updated"
FROM stock_prices
GROUP BY ticker
ORDER BY ticker;


-- 5) Prediction Residuals (Predicted - Actual)
SELECT 
    predicted_date AS ds,
    ticker,
    model_version,
    (predicted_close - actual_close) AS residual_error
FROM model_predictions
WHERE actual_close IS NOT NULL
ORDER BY ds ASC;

-- 6) Prediction Error Distribution (Histogram)
SELECT 
    ticker,
    ROUND((predicted_close - actual_close)::numeric, 0) AS prediction_error
FROM model_predictions
WHERE actual_close IS NOT NULL;



-- 7) Card: Latest Close Price
WITH latest AS (
  SELECT ticker, MAX(date) AS max_date 
  FROM stock_prices 
  GROUP BY ticker
)
SELECT 
  s.ticker, 
  s.date AS ds, 
  s.close
FROM stock_prices s
JOIN latest l ON s.ticker = l.ticker AND s.date = l.max_date;

-- 8) Card: Latest Predicted Price
WITH latest AS (
  SELECT ticker, MAX(predicted_date) AS max_date 
  FROM model_predictions 
  GROUP BY ticker
)
SELECT 
  m.ticker, 
  m.predicted_date AS ds, 
  m.predicted_close
FROM model_predictions m
JOIN latest l ON m.ticker = l.ticker AND m.predicted_date = l.max_date;

-- 9) Model Performance Leaderboard (MAE & RMSE)
SELECT 
    ticker,                    -- Invisible hook for the Native Dashboard Filter
    model_version AS "Model Version",
    
    -- Mean Absolute Error (MAE)
    ROUND(AVG(ABS(predicted_close - actual_close))::numeric, 4) AS "MAE ($)",
    
    -- Root Mean Squared Error (RMSE)
    ROUND(SQRT(AVG(POWER(predicted_close - actual_close, 2)))::numeric, 4) AS "RMSE ($)",

    MIN(predicted_date) AS "Creation Date"
    
FROM model_predictions
WHERE actual_close IS NOT NULL
GROUP BY ticker, model_version
ORDER BY "Creation Date" DESC;