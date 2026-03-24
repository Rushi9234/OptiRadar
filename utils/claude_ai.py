import os
import logging
from groq import Groq
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = Groq(api_key=os.getenv("GROQ_API_KEY"))


def generate_alert_card(stock_result: dict) -> str:
    ticker = stock_result["ticker"].replace(".NS", "")
    score = stock_result["score"]
    bucket = stock_result["bucket"]
    signals = stock_result["signals_fired"]
    price = stock_result.get("current_price", 0)

    bulk = stock_result["bulk_deal"]["details"]
    volume = stock_result["volume_spike"]["details"]
    insider = stock_result["insider_trade"]["details"]
    technical = stock_result["technical"]["details"]
    sentiment = stock_result["sentiment"]["details"]

    prompt = f"""You are OptiRadar, an AI stock signal analyst for Indian retail investors on NSE.

A stock has triggered a signal. Generate a concise 4-sentence alert card.

Stock: {ticker}
Current Price: Rs {price}
OptiRadar Score: {score}/100
Signal Bucket: {bucket}
Signals Fired: {', '.join(signals) if signals else 'None'}

Signal Details:
- Bulk Deal: {bulk}
- Volume: {volume}
- Insider Trade: {insider}
- Technical: {technical}
- Sentiment: {sentiment}

Write exactly 4 sentences:
1. What was detected (specific signals that fired)
2. What this historically means for price movement
3. What to watch in the next 1-3 days
4. One-line recommendation (Buy/Watch/Avoid with reason)

Be specific, use numbers, speak like a sharp analyst. No fluff."""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        card = response.choices[0].message.content.strip()
        logger.info(f"Alert card generated for {ticker}")
        return card
    except Exception as e:
        logger.error(f"Error generating alert card for {ticker}: {e}")
        return f"Signal detected for {ticker} with score {score}/100. Signals: {', '.join(signals)}."


def generate_daily_digest(all_results: list) -> str:
    hot_stocks = [r for r in all_results if r["bucket"] in ["Hot", "Very Hot"]]
    top_5 = sorted(all_results, key=lambda x: x["score"], reverse=True)[:5]

    top_lines = "\n".join([
        f"- {r['ticker'].replace('.NS','')} | Score: {r['score']} | {r['bucket']} | Signals: {', '.join(r['signals_fired']) or 'None'}"
        for r in top_5
    ])

    prompt = f"""You are OptiRadar. Generate a structured end-of-day market digest for Indian retail investors.

Today's scan results:
- Total stocks scanned: {len(all_results)}
- Hot/Very Hot signals: {len(hot_stocks)}

Top 5 stocks by score:
{top_lines}

Write a digest with these exact sections:
1. MARKET PULSE (2 sentences on overall signal activity today)
2. TOP OPPORTUNITIES (bullet points for each hot stock with key reason)
3. WHAT TO WATCH TOMORROW (2-3 specific things to monitor)
4. RISK NOTE (1 sentence standard disclaimer)

Be sharp, data-driven, and useful for a retail investor. No generic advice."""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        digest = response.choices[0].message.content.strip()
        logger.info("Daily digest generated")
        return digest
    except Exception as e:
        logger.error(f"Error generating daily digest: {e}")
        return "Daily digest unavailable. Check API key."


if __name__ == "__main__":
    from data.fetcher import fetch_stock_data, get_current_price
    from data.nse_scraper import fetch_bulk_deals, fetch_insider_trades
    from data.news_fetcher import fetch_news_for_stock
    from signals.scorer import score_stock

    print("Testing Groq AI alert card generation...")
    price_df = fetch_stock_data("HDFCBANK.NS")
    bulk_df = fetch_bulk_deals()
    insider_df = fetch_insider_trades()
    news_df = fetch_news_for_stock("HDFCBANK.NS")

    result = score_stock("HDFCBANK.NS", price_df, bulk_df, insider_df, news_df)
    result["current_price"] = get_current_price("HDFCBANK.NS")
    card = generate_alert_card(result)
    print(card)
