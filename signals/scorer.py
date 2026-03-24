import pandas as pd
import logging
from signals.bulk_deal import detect_bulk_deal
from signals.volume_spike import detect_volume_spike
from signals.insider_trade import detect_insider_trade
from signals.technical import detect_technical
from signals.sentiment import detect_sentiment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEIGHTS = {
    "bulk_deal": 0.30,
    "volume_spike": 0.25,
    "insider_trade": 0.25,
    "technical": 0.15,
    "sentiment": 0.05,
}

BUCKET_THRESHOLDS = {
    "Very Hot": 80,
    "Hot": 60,
    "Warm": 30,
    "Cold": 0,
}


def get_bucket(score: float) -> str:
    if score >= 80:
        return "Very Hot"
    elif score >= 60:
        return "Hot"
    elif score >= 30:
        return "Warm"
    else:
        return "Cold"


def score_stock(
    ticker: str,
    price_df: pd.DataFrame,
    bulk_df: pd.DataFrame,
    insider_df: pd.DataFrame,
    news_df: pd.DataFrame,
) -> dict:
    logger.info(f"Scoring {ticker}...")

    bulk_result = detect_bulk_deal(ticker, bulk_df)
    volume_result = detect_volume_spike(price_df)
    insider_result = detect_insider_trade(ticker, insider_df)
    technical_result = detect_technical(price_df)
    sentiment_result = detect_sentiment(ticker, news_df)

    weighted_score = (
        bulk_result["score"] * WEIGHTS["bulk_deal"] +
        volume_result["score"] * WEIGHTS["volume_spike"] +
        insider_result["score"] * WEIGHTS["insider_trade"] +
        technical_result["score"] * WEIGHTS["technical"] +
        sentiment_result["score"] * WEIGHTS["sentiment"]
    )

    final_score = round(weighted_score * 100, 2)
    bucket = get_bucket(final_score)

    signals_fired = []
    if bulk_result["fired"]:
        signals_fired.append(f"Bulk Deal ({bulk_result['score']:.2f})")
    if volume_result["fired"]:
        signals_fired.append(f"Volume Spike ({volume_result['score']:.2f})")
    if insider_result["fired"]:
        signals_fired.append(f"Insider Trade ({insider_result['score']:.2f})")
    if technical_result["fired"]:
        signals_fired.append(f"Technical ({technical_result['score']:.2f})")
    if sentiment_result["fired"]:
        signals_fired.append(f"Sentiment ({sentiment_result['score']:.2f})")

    result = {
        "ticker": ticker,
        "score": final_score,
        "bucket": bucket,
        "signals_fired": signals_fired,
        "signal_count": len(signals_fired),
        "bulk_deal": bulk_result,
        "volume_spike": volume_result,
        "insider_trade": insider_result,
        "technical": technical_result,
        "sentiment": sentiment_result,
    }

    logger.info(f"{ticker} | Score: {final_score} | Bucket: {bucket} | Signals: {signals_fired}")
    return result


def score_all_stocks(
    stock_data: dict,
    bulk_df: pd.DataFrame,
    insider_df: pd.DataFrame,
    news_data: dict,
) -> list:
    results = []
    for ticker, price_df in stock_data.items():
        news_df = news_data.get(ticker, pd.DataFrame())
        result = score_stock(ticker, price_df, bulk_df, insider_df, news_df)
        results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"Scored {len(results)} stocks total")
    return results


if __name__ == "__main__":
    from data.fetcher import fetch_stock_data
    from data.nse_scraper import fetch_bulk_deals, fetch_insider_trades
    from data.news_fetcher import fetch_news_for_stock

    print("Testing master scorer on RELIANCE.NS and TCS.NS...")

    tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]
    stock_data = {t: fetch_stock_data(t) for t in tickers}
    bulk_df = fetch_bulk_deals()
    insider_df = fetch_insider_trades()
    news_data = {t: fetch_news_for_stock(t) for t in tickers}

    results = score_all_stocks(stock_data, bulk_df, insider_df, news_data)

    print("\n===== OPTI RADAR SCORES =====")
    for r in results:
        print(f"{r['ticker']:20s} | Score: {r['score']:6.2f} | Bucket: {r['bucket']:10s} | Signals: {r['signals_fired']}")
