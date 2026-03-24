import yfinance as yf
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Top 50 NSE stocks we monitor
NSE_STOCKS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "BAJFINANCE.NS", "NESTLEIND.NS", "WIPRO.NS", "ULTRACEMCO.NS",
    "POWERGRID.NS", "NTPC.NS", "TECHM.NS", "HCLTECH.NS", "BAJAJFINSV.NS",
    "ONGC.NS", "COALINDIA.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "JSWSTEEL.NS",
    "ADANIENT.NS", "ADANIPORTS.NS", "DIVISLAB.NS", "DRREDDY.NS", "CIPLA.NS",
    "EICHERMOT.NS", "HEROMOTOCO.NS", "BRITANNIA.NS", "DABUR.NS", "MARICO.NS",
    "PIDILITIND.NS", "BERGEPAINT.NS", "HAVELLS.NS", "VOLTAS.NS", "TATACONSUM.NS",
    "APOLLOHOSP.NS", "FORTIS.NS", "MCDOWELL-N.NS", "UBL.NS", "INDUSINDBK.NS"
]

def fetch_stock_data(ticker: str, period: str = "35d", interval: str = "1d") -> pd.DataFrame:
    """Fetch OHLCV data for a single stock."""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)
        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()
        df.index = pd.to_datetime(df.index)
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        df['ticker'] = ticker
        df['fetched_at'] = datetime.now()
        logger.info(f"Fetched {len(df)} rows for {ticker}")
        return df
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

def fetch_all_stocks() -> dict:
    """Fetch data for all NSE stocks. Returns dict of {ticker: DataFrame}."""
    all_data = {}
    logger.info(f"Starting fetch for {len(NSE_STOCKS)} stocks...")
    for ticker in NSE_STOCKS:
        df = fetch_stock_data(ticker)
        if not df.empty:
            all_data[ticker] = df
    logger.info(f"Successfully fetched {len(all_data)}/{len(NSE_STOCKS)} stocks")
    return all_data

def get_current_price(ticker: str) -> float:
    """Get the latest closing price for a stock."""
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d")
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 2)
        return 0.0
    except Exception as e:
        logger.error(f"Error getting price for {ticker}: {e}")
        return 0.0

if __name__ == "__main__":
    # Test the fetcher
    print("Testing fetcher on RELIANCE.NS...")
    df = fetch_stock_data("RELIANCE.NS")
    print(df.tail(3))
    print(f"\nCurrent price: ₹{get_current_price('RELIANCE.NS')}")