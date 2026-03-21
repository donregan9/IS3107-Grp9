"""
LSTM model for next-day stock close price prediction.

Training  : called by the weekly Airflow DAG (lstm_weekly_training).
Prediction: called by the daily Airflow DAG (lstm_daily_prediction).

Model artefacts are saved to  <AIRFLOW_HOME>/models/<ticker>/
  - lstm_model.keras   : trained Keras model
  - scaler.pkl         : fitted MinMaxScaler (all features + target)
  - model_version.txt  : version string used as FK in model_metadata
"""

import os
import pickle
import logging
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# DB helpers (duplicated here so this script is self-contained)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_USER     = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME     = os.getenv('DB_NAME', 'airflow')
DB_PORT     = os.getenv('DB_PORT', '5432')

log = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEATURE_COLS = [
    'close', 'daily_return', 'sma_20', 'sma_50',
    'ema_12', 'ema_26', 'macd', 'macd_signal',
    'rsi_14', 'bb_upper', 'bb_lower', 'volatility_14', 'volume_sma_20',
    'bb_width', 'volume_ratio_20', 'return_3d', 'return_5d'
]

SEQUENCE_LEN = 30          # look-back window (trading days)
EPOCHS       = 50
BATCH_SIZE   = 32
VALIDATION   = 0.15        # fraction of training data used for validation


# ---------------------------------------------------------------------------
# Model-artefact paths
# ---------------------------------------------------------------------------
def _model_dir(ticker: str) -> str:
    airflow_home = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
    path = os.path.join(airflow_home, 'models', ticker)
    os.makedirs(path, exist_ok=True)
    return path


def _model_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'lstm_model.keras')


def _scaler_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'scaler.pkl')


def _version_path(ticker: str) -> str:
    return os.path.join(_model_dir(ticker), 'model_version.txt')


def _current_version(ticker: str) -> str | None:
    """Return the version string stored on disk, or None if no model trained yet."""
    p = _version_path(ticker)
    if os.path.exists(p):
        with open(p) as f:
            return f.read().strip()
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def _load_features(ticker: str) -> pd.DataFrame:
    """Load all rows from stock_features for `ticker`, sorted by date."""
    conn = _get_conn()
    df = pd.read_sql(
        f"""
        SELECT date, {', '.join(FEATURE_COLS)}
        FROM stock_features
        WHERE ticker = %s
        ORDER BY date ASC
        """,
        conn, params=(ticker,)
    )
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df


# ---------------------------------------------------------------------------
# Sequence builder
# ---------------------------------------------------------------------------
def _build_sequences(scaled: np.ndarray, seq_len: int):
    """
    Turn a 2-D scaled array (rows = days, cols = features) into
    (X, y) arrays suitable for LSTM.

    X shape: (n_samples, seq_len, n_features)
    y shape: (n_samples,)  — next-day close (index 0 in FEATURE_COLS)
    """
    X, y = [], []
    close_idx = 0   # 'close' is the first column in FEATURE_COLS
    for i in range(seq_len, len(scaled)):
        X.append(scaled[i - seq_len:i, :])
        y.append(scaled[i, close_idx])
    return np.array(X), np.array(y)


# ---------------------------------------------------------------------------
# Build Keras model
# ---------------------------------------------------------------------------
def _build_model(n_features: int):
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.optimizers import Adam

    model = Sequential([
        LSTM(128, return_sequences=True, input_shape=(SEQUENCE_LEN, n_features)),
        Dropout(0.2),
        LSTM(64, return_sequences=False),
        Dropout(0.2),
        Dense(32, activation='relu'),
        Dense(1)
    ])
    model.compile(optimizer=Adam(learning_rate=1e-3), loss='mse', metrics=['mae'])
    return model


# ---------------------------------------------------------------------------
# PUBLIC: train_model
# ---------------------------------------------------------------------------
def train_model(ticker: str) -> dict:
    """
    Trains (or retrains) the LSTM on all available stock_features data.
    Saves model + scaler to disk and logs metadata to the DB.

    Returns a dict with training metrics.
    """
    from sklearn.preprocessing import MinMaxScaler
    from tensorflow.keras.callbacks import EarlyStopping

    log.info("Loading features for %s...", ticker)
    df = _load_features(ticker)

    min_rows = SEQUENCE_LEN + 10
    if len(df) < min_rows:
        raise ValueError(
            f"Not enough data to train: need ≥{min_rows} rows, have {len(df)}"
        )

    log.info("Training on %d rows (%s → %s)", len(df),
             df['date'].iloc[0].date(), df['date'].iloc[-1].date())

    # Scale all feature columns together
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled = scaler.fit_transform(df[FEATURE_COLS].values)

    X, y = _build_sequences(scaled, SEQUENCE_LEN)

    # Chronological train / validation split (no shuffle — time-series)
    split = int(len(X) * (1 - VALIDATION))
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    model = _build_model(n_features=len(FEATURE_COLS))

    early_stop = EarlyStopping(
        monitor='val_loss', patience=10, restore_best_weights=True
    )

    history = model.fit(
        X_train, y_train,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        validation_data=(X_val, y_val),
        callbacks=[early_stop],
        verbose=1
    )

    # Evaluate on the validation set
    val_loss, val_mae = model.evaluate(X_val, y_val, verbose=0)
    val_rmse = float(np.sqrt(val_loss))
    val_mae  = float(val_mae)

    # Inverse-transform to get errors in real price units
    # We only care about the close column (idx 0); pad the rest with zeros
    def inv_close(arr_1d):
        pad = np.zeros((len(arr_1d), len(FEATURE_COLS)))
        pad[:, 0] = arr_1d
        return scaler.inverse_transform(pad)[:, 0]

    y_val_pred   = model.predict(X_val, verbose=0).flatten()
    pred_prices  = inv_close(y_val_pred)
    actual_prices = inv_close(y_val)
    mae_price = float(np.mean(np.abs(pred_prices - actual_prices)))
    rmse_price = float(np.sqrt(np.mean((pred_prices - actual_prices) ** 2)))

    log.info("Val MAE: $%.4f  Val RMSE: $%.4f", mae_price, rmse_price)

    # Version string: ticker + ISO timestamp
    version = f"{ticker}_v{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

    # Persist artefacts
    model.save(_model_path(ticker))
    with open(_scaler_path(ticker), 'wb') as f:
        pickle.dump(scaler, f)
    with open(_version_path(ticker), 'w') as f:
        f.write(version)

    log.info("Saved model to %s", _model_path(ticker))

    # Write metadata to DB
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO model_metadata
            (ticker, model_version, trained_at, training_start, training_end, mae, rmse, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (model_version) DO UPDATE SET
            trained_at     = EXCLUDED.trained_at,
            training_start = EXCLUDED.training_start,
            training_end   = EXCLUDED.training_end,
            mae            = EXCLUDED.mae,
            rmse           = EXCLUDED.rmse,
            notes          = EXCLUDED.notes
    """, (
        ticker, version, datetime.utcnow(),
        df['date'].iloc[0].date(), df['date'].iloc[-1].date(),
        mae_price, rmse_price,
        f"LSTM 128-64 | seq={SEQUENCE_LEN} epochs={len(history.epoch)+1}"
    ))
    conn.commit()
    cur.close()
    conn.close()

    return {
        'version': version,
        'mae_price': mae_price,
        'rmse_price': rmse_price,
        'training_rows': len(df),
        'epochs_run': len(history.epoch) + 1,
    }


# ---------------------------------------------------------------------------
# PUBLIC: predict_next_day
# ---------------------------------------------------------------------------
def predict_next_day(ticker: str) -> dict:
    """
    Uses the latest saved model to predict the next trading day's close price.
    - Loads the most recent SEQUENCE_LEN rows from stock_features.
    - Writes the prediction to model_predictions.
    - Back-fills actual_close for the previous prediction row.

    Returns a dict with predicted date + price.
    """
    from tensorflow.keras.models import load_model

    version = _current_version(ticker)
    if version is None:
        raise FileNotFoundError(
            f"No trained model found for {ticker}. Run the weekly training DAG first."
        )

    # --- Load artefacts ---
    model  = load_model(_model_path(ticker))
    with open(_scaler_path(ticker), 'rb') as f:
        scaler = pickle.load(f)

    # Backward compatibility: older trained models/scalers may have fewer
    # features than the current FEATURE_COLS list.
    scaler_feature_count = int(getattr(scaler, 'n_features_in_', len(FEATURE_COLS)))
    if scaler_feature_count > len(FEATURE_COLS):
        raise ValueError(
            f"Scaler expects {scaler_feature_count} features, but only {len(FEATURE_COLS)} are configured."
        )
    active_feature_cols = FEATURE_COLS[:scaler_feature_count]
    if scaler_feature_count != len(FEATURE_COLS):
        log.warning(
            "Using %d/%d features for prediction to match saved scaler/model. "
            "Run weekly training to upgrade model to the latest feature set.",
            scaler_feature_count,
            len(FEATURE_COLS),
        )

    # --- Load the last SEQUENCE_LEN rows ---
    conn = _get_conn()
    df = pd.read_sql(
        f"""
        SELECT date, {', '.join(active_feature_cols)}
        FROM stock_features
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT %s
        """,
        conn, params=(ticker, SEQUENCE_LEN)
    )
    conn.close()

    if len(df) < SEQUENCE_LEN:
        raise ValueError(
            f"Need {SEQUENCE_LEN} rows for prediction, only {len(df)} available."
        )

    df = df.sort_values('date').reset_index(drop=True)
    latest_date = df['date'].iloc[-1]

    # Predict the *next* trading day (skip weekends)
    predicted_date = _next_trading_day(latest_date)

    # Scale and build single sequence
    scaled = scaler.transform(df[active_feature_cols].values)
    X = scaled.reshape(1, SEQUENCE_LEN, len(active_feature_cols))

    raw_pred = model.predict(X, verbose=0)[0, 0]

    # Inverse-transform the predicted close
    pad = np.zeros((1, len(active_feature_cols)))
    pad[0, 0] = raw_pred
    predicted_close = float(scaler.inverse_transform(pad)[0, 0])

    log.info("Prediction for %s on %s: $%.4f", ticker, predicted_date, predicted_close)

    # --- Write to DB ---
    conn = _get_conn()
    cur  = conn.cursor()

    # Replace any existing prediction for this ticker+date (re-run safety)
    cur.execute("""
        DELETE FROM model_predictions
        WHERE ticker = %s AND predicted_date = %s
    """, (ticker, predicted_date))
    cur.execute("""
        INSERT INTO model_predictions (ticker, predicted_date, predicted_close, model_version)
        VALUES (%s, %s, %s, %s)
    """, (ticker, predicted_date, predicted_close, version))

    # Back-fill actual_close for yesterday's prediction row
    yesterday = latest_date.date() if hasattr(latest_date, 'date') else latest_date
    cur.execute("""
        UPDATE model_predictions mp
        SET actual_close = sp.close
        FROM stock_prices sp
        WHERE mp.ticker         = %s
          AND mp.predicted_date = %s
          AND sp.ticker         = %s
          AND DATE(sp.date)     = %s
          AND mp.actual_close   IS NULL
    """, (ticker, yesterday, ticker, yesterday))

    conn.commit()
    cur.close()
    conn.close()

    return {
        'ticker':          ticker,
        'predicted_date':  str(predicted_date),
        'predicted_close': predicted_close,
        'model_version':   version,
        'based_on_date':   str(latest_date.date() if hasattr(latest_date, 'time') else latest_date),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _next_trading_day(dt) -> date:
    """Return the next Mon–Fri after `dt` (skips Saturday and Sunday)."""
    if hasattr(dt, 'date'):
        dt = dt.date()
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:          # 5=Sat, 6=Sun
        next_day += timedelta(days=1)
    return next_day
