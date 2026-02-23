"""
Stock data extraction and processing module
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_stock_prices(ticker, period="1d", interval="1m"):
    """
    Fetch historical stock prices from Yahoo Finance
    
    Args:
        ticker (str): Stock ticker symbol (e.g., 'AAPL')
        period (str): Period for data (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        interval (str): Interval for data (1m, 5m, 15m, 30m, 60m, 1d, 1wk, 1mo)
    
    Returns:
        pd.DataFrame: Stock prices with columns (Open, High, Low, Close, Volume)
    """
    try:
        data = yf.download(tickers=ticker, period=period, interval=interval, progress=False)
        data.reset_index(inplace=True)
        data['ticker'] = ticker
        return data
    except Exception as e:
        print(f"Error fetching {ticker}: {str(e)}")
        return None

def calculate_momentum(data, window=20):
    """
    Calculate momentum indicators
    
    Args:
        data (pd.DataFrame): Stock price data with 'Close' column
        window (int): Window size for calculations
    
    Returns:
        pd.DataFrame: Data with momentum indicators added
    """
    data['sma'] = data['Close'].rolling(window=window).mean()
    data['momentum'] = data['Close'] - data['Close'].shift(window)
    data['rsi'] = calculate_rsi(data['Close'], window=14)
    return data

def calculate_rsi(prices, window=14):
    """Calculate Relative Strength Index"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def save_to_csv(data, filename):
    """Save dataframe to CSV"""
    data.to_csv(filename, index=False)
    print(f"Saved to {filename}")

if __name__ == "__main__":
    # Example usage
    aapl_data = fetch_stock_prices('AAPL', period='1mo', interval='1d')
    if aapl_data is not None:
        aapl_data = calculate_momentum(aapl_data)
        save_to_csv(aapl_data, '/opt/airflow/data/aapl_momentum.csv')
