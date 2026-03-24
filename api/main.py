import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import logging
from datetime import datetime

from data.fetcher import fetch_stock_data, fetch_all_stocks, get_current_price, NSE_STOCKS
from data.nse_scraper import fetch_bulk_deals, fetch_insider_trades
from data.news_fetcher import fetch_news_for_stock, fetch_news_for_all
from signals.scorer import score_stock, score_all_stocks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="OptiRadar API",
    description="AI-powered NSE stock signal radar",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache to avoid re-fetching every request
_cache = {
    "bulk_df": pd.DataFrame(),
    "insider_df": pd.DataFrame(),
    "stock_data": {},
    "news_data": {},
    "last_refresh": None,
}


def refresh_cache():
    logger.info("Refreshing data cache...")
    _cache["bulk_df"] = fetch_bulk_deals()
    _cache["insider_df"] = fetch_insider_trades()
    _cache["stock_data"] = fetch_all_stocks()
    _cache["news_data"] = fetch_news_for_all(NSE_STOCKS[:10])
    _cache["last_refresh"] = datetime.now().isoformat()
    logger.info("Cache refresh complete")


@app.get("/")
def root():
    return {
        "name": "OptiRadar API",
        "version": "2.0.0",
        "status": "live",
        "last_refresh": _cache["last_refresh"],
        "endpoints": ["/scan/all", "/scan/{ticker}", "/refresh", "/health"]
    }


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.post("/refresh")
def refresh():
    refresh_cache()
    return {
        "status": "refreshed",
        "stocks_loaded": len(_cache["stock_data"]),
        "insider_trades": len(_cache["insider_df"]),
        "time": _cache["last_refresh"]
    }


@app.get("/scan/{ticker}")
def scan_single(ticker: str):
    try:
        ticker = ticker.upper()
        if not ticker.endswith(".NS"):
            ticker = ticker + ".NS"

        price_df = fetch_stock_data(ticker)
        if price_df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

        bulk_df = _cache["bulk_df"] if not _cache["bulk_df"].empty else fetch_bulk_deals()
        insider_df = _cache["insider_df"] if not _cache["insider_df"].empty else fetch_insider_trades()
        news_df = fetch_news_for_stock(ticker)

        result = score_stock(ticker, price_df, bulk_df, insider_df, news_df)

        return {
            "ticker": result["ticker"],
            "score": result["score"],
            "bucket": result["bucket"],
            "signals_fired": result["signals_fired"],
            "signal_count": result["signal_count"],
            "current_price": get_current_price(ticker),
            "details": {
                "bulk_deal": result["bulk_deal"]["details"],
                "volume_spike": result["volume_spike"]["details"],
                "insider_trade": result["insider_trade"]["details"],
                "technical": result["technical"]["details"],
                "sentiment": result["sentiment"]["details"],
            },
            "scanned_at": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scanning {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scan/all/stocks")
def scan_all():
    try:
        if not _cache["stock_data"]:
            refresh_cache()

        results = score_all_stocks(
            _cache["stock_data"],
            _cache["bulk_df"],
            _cache["insider_df"],
            _cache["news_data"],
        )

        output = []
        for r in results:
            output.append({
                "ticker": r["ticker"],
                "score": r["score"],
                "bucket": r["bucket"],
                "signals_fired": r["signals_fired"],
                "signal_count": r["signal_count"],
                "current_price": get_current_price(r["ticker"]),
            })

        hot_count = sum(1 for r in output if r["bucket"] in ["Hot", "Very Hot"])

        return {
            "total_stocks": len(output),
            "hot_signals": hot_count,
            "scanned_at": datetime.now().isoformat(),
            "results": output
        }

    except Exception as e:
        logger.error(f"Error in scan all: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/{ticker}")
def get_price(ticker: str):
    ticker = ticker.upper()
    if not ticker.endswith(".NS"):
        ticker = ticker + ".NS"
    price = get_current_price(ticker)
    return {"ticker": ticker, "price": price}
