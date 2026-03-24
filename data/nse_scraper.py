import requests
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

def get_nse_session() -> requests.Session:
    """Create a session with NSE cookies — required to avoid 401 errors."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
        session.get("https://www.nseindia.com/market-data/bulk-deals", timeout=10)
    except Exception as e:
        logger.warning(f"Session init warning: {e}")
    return session

def fetch_bulk_deals() -> pd.DataFrame:
    """Fetch today's bulk deals from NSE."""
    try:
        session = get_nse_session()
        url = "https://www.nseindia.com/api/bulk-deal-archives?optionType=bulk_deals&year=2026&month=&fromDate=&toDate="
        response = session.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                df = pd.DataFrame(data["data"])
                df['fetched_at'] = datetime.now()
                logger.info(f"Fetched {len(df)} bulk deals")
                return df
        logger.warning(f"Bulk deals returned status: {response.status_code}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching bulk deals: {e}")
        return pd.DataFrame()

def fetch_insider_trades() -> pd.DataFrame:
    """Fetch recent insider trading disclosures from NSE."""
    try:
        session = get_nse_session()
        url = "https://www.nseindia.com/api/corporates-pit?index=equities&period=oneMonth"
        response = session.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                df = pd.DataFrame(data["data"])
                df['fetched_at'] = datetime.now()
                logger.info(f"Fetched {len(df)} insider trades")
                return df
        logger.warning(f"Insider trades returned status: {response.status_code}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching insider trades: {e}")
        return pd.DataFrame()

def check_bulk_deal_for_stock(ticker: str, bulk_df: pd.DataFrame) -> dict:
    """Check if a specific stock has a bulk deal today."""
    result = {"has_deal": False, "stake_change": 0.0, "details": ""}
    if bulk_df.empty:
        return result
    # Clean ticker — remove .NS suffix
    symbol = ticker.replace(".NS", "")
    try:
        matched = bulk_df[bulk_df['symbol'].str.upper() == symbol.upper()]
        if not matched.empty:
            result["has_deal"] = True
            result["details"] = matched.iloc[0].to_dict()
            # Try to get quantity as proxy for stake change
            if 'quantity' in matched.columns:
                result["stake_change"] = float(str(matched.iloc[0]['quantity']).replace(',', '') or 0) / 1e7
        return result
    except Exception as e:
        logger.error(f"Error checking bulk deal for {symbol}: {e}")
        return result

def check_insider_trade_for_stock(ticker: str, insider_df: pd.DataFrame) -> dict:
    """Check if a specific stock has insider trades in last 7 days."""
    result = {"has_trade": False, "trade_type": "", "details": ""}
    if insider_df.empty:
        return result
    symbol = ticker.replace(".NS", "")
    try:
        matched = insider_df[insider_df['symbol'].str.upper() == symbol.upper()]
        if not matched.empty:
            result["has_trade"] = True
            if 'acqMode' in matched.columns:
                result["trade_type"] = str(matched.iloc[0]['acqMode'])
            result["details"] = matched.iloc[0].to_dict()
        return result
    except Exception as e:
        logger.error(f"Error checking insider trade for {symbol}: {e}")
        return result

if __name__ == "__main__":
    print("Testing NSE scraper...")
    print("\n--- Bulk Deals ---")
    bulk = fetch_bulk_deals()
    if not bulk.empty:
        print(bulk.head(3).to_string())
    else:
        print("No bulk deals data (market may be closed or NSE blocking)")

    print("\n--- Insider Trades ---")
    insider = fetch_insider_trades()
    if not insider.empty:
        print(insider.head(3).to_string())
    else:
        print("No insider trades data returned")