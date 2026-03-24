import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# Map ticker to company search terms for better news results
TICKER_TO_COMPANY = {
    "RELIANCE.NS": "Reliance Industries",
    "TCS.NS": "TCS Tata Consultancy",
    "HDFCBANK.NS": "HDFC Bank",
    "INFY.NS": "Infosys",
    "ICICIBANK.NS": "ICICI Bank",
    "HINDUNILVR.NS": "Hindustan Unilever HUL",
    "ITC.NS": "ITC Limited",
    "SBIN.NS": "State Bank of India SBI",
    "BHARTIARTL.NS": "Bharti Airtel",
    "KOTAKBANK.NS": "Kotak Mahindra Bank",
    "LT.NS": "Larsen Toubro L&T",
    "AXISBANK.NS": "Axis Bank",
    "ASIANPAINT.NS": "Asian Paints",
    "MARUTI.NS": "Maruti Suzuki",
    "SUNPHARMA.NS": "Sun Pharma",
    "TITAN.NS": "Titan Company",
    "BAJFINANCE.NS": "Bajaj Finance",
    "NESTLEIND.NS": "Nestle India",
    "WIPRO.NS": "Wipro",
    "ULTRACEMCO.NS": "UltraTech Cement",
    "POWERGRID.NS": "Power Grid Corporation",
    "NTPC.NS": "NTPC Limited",
    "TECHM.NS": "Tech Mahindra",
    "HCLTECH.NS": "HCL Technologies",
    "BAJAJFINSV.NS": "Bajaj Finserv",
    "ONGC.NS": "ONGC Oil Natural Gas",
    "COALINDIA.NS": "Coal India",
    "TATAMOTORS.NS": "Tata Motors",
    "TATASTEEL.NS": "Tata Steel",
    "JSWSTEEL.NS": "JSW Steel",
    "ADANIENT.NS": "Adani Enterprises",
    "ADANIPORTS.NS": "Adani Ports",
    "DIVISLAB.NS": "Divi's Laboratories",
    "DRREDDY.NS": "Dr Reddy's Laboratories",
    "CIPLA.NS": "Cipla",
    "EICHERMOT.NS": "Eicher Motors Royal Enfield",
    "HEROMOTOCO.NS": "Hero MotoCorp",
    "BRITANNIA.NS": "Britannia Industries",
    "DABUR.NS": "Dabur India",
    "MARICO.NS": "Marico",
    "PIDILITIND.NS": "Pidilite Fevicol",
    "BERGEPAINT.NS": "Berger Paints",
    "HAVELLS.NS": "Havells India",
    "VOLTAS.NS": "Voltas",
    "TATACONSUM.NS": "Tata Consumer Products",
    "APOLLOHOSP.NS": "Apollo Hospitals",
    "FORTIS.NS": "Fortis Healthcare",
    "MCDOWELL-N.NS": "United Spirits McDowell",
    "UBL.NS": "United Breweries Kingfisher",
    "INDUSINDBK.NS": "IndusInd Bank",
}

def fetch_news_for_stock(ticker: str, days_back: int = 1) -> pd.DataFrame:
    """Fetch latest news headlines for a stock using NewsAPI."""
    if not NEWS_API_KEY:
        logger.error("NEWS_API_KEY not found in .env")
        return pd.DataFrame()

    company_name = TICKER_TO_COMPANY.get(ticker, ticker.replace(".NS", ""))
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": company_name,
            "from": from_date,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 10,
            "apiKey": NEWS_API_KEY,
        }
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            articles = response.json().get("articles", [])
            if articles:
                df = pd.DataFrame([{
                    "ticker": ticker,
                    "title": a.get("title", ""),
                    "description": a.get("description", ""),
                    "source": a.get("source", {}).get("name", ""),
                    "published_at": a.get("publishedAt", ""),
                    "url": a.get("url", ""),
                } for a in articles])
                logger.info(f"Fetched {len(df)} articles for {ticker}")
                return df
        logger.warning(f"NewsAPI status {response.status_code} for {ticker}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching news for {ticker}: {e}")
        return pd.DataFrame()

def fetch_news_for_all(tickers: list) -> dict:
    """Fetch news for multiple stocks. Returns dict of {ticker: DataFrame}."""
    all_news = {}
    for ticker in tickers:
        df = fetch_news_for_stock(ticker)
        if not df.empty:
            all_news[ticker] = df
    logger.info(f"News fetched for {len(all_news)}/{len(tickers)} stocks")
    return all_news

def get_latest_headline(ticker: str, news_dict: dict) -> str:
    """Get the most recent headline for a stock."""
    if ticker not in news_dict or news_dict[ticker].empty:
        return "No recent news"
    return news_dict[ticker].iloc[0]['title']

if __name__ == "__main__":
    print("Testing NewsAPI fetcher...")
    df = fetch_news_for_stock("RELIANCE.NS")
    if not df.empty:
        print(f"\nFound {len(df)} articles for Reliance:")
        for _, row in df.iterrows():
            print(f"  - {row['title']}")
    else:
        print("No news returned — check your NEWS_API_KEY in .env")