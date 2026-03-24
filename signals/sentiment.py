import pandas as pd
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

analyzer = SentimentIntensityAnalyzer()


def detect_sentiment(ticker: str, news_df: pd.DataFrame) -> dict:
    result = {
        "signal": "sentiment",
        "score": 0.0,
        "fired": False,
        "sentiment_label": "neutral",
        "details": ""
    }

    if news_df is None or news_df.empty:
        result["details"] = "No news data available"
        return result

    try:
        sentiments = []

        for _, row in news_df.iterrows():
            title = str(row.get("title", ""))
            description = str(row.get("description", ""))
            text = f"{title}. {description}".strip()
            if not text or text == ".":
                continue
            scores = analyzer.polarity_scores(text)
            sentiments.append(scores["compound"])

        if not sentiments:
            result["details"] = "No valid headlines to analyze"
            return result

        avg_sentiment = sum(sentiments) / len(sentiments)
        abs_sentiment = abs(avg_sentiment)

        score = min(abs_sentiment * 1.5, 1.0)

        if avg_sentiment >= 0.20:
            label = "positive"
        elif avg_sentiment <= -0.20:
            label = "negative"
        else:
            label = "neutral"

        result["score"] = round(score, 4)
        result["fired"] = score >= 0.30
        result["sentiment_label"] = label
        result["details"] = (
            f"Headlines analyzed: {len(sentiments)} | "
            f"Avg sentiment: {avg_sentiment:.4f} | "
            f"Label: {label} | "
            f"Score: {score:.4f}"
        )

        logger.info(f"Sentiment for {ticker}: {label} score={score:.4f}")
        return result

    except Exception as e:
        logger.error(f"Error in sentiment detection for {ticker}: {e}")
        result["details"] = f"Error: {e}"
        return result


if __name__ == "__main__":
    from data.news_fetcher import fetch_news_for_stock
    print("Testing sentiment detector on RELIANCE.NS...")
    news_df = fetch_news_for_stock("RELIANCE.NS")
    result = detect_sentiment("RELIANCE.NS", news_df)
    print(f"Signal fired: {result['fired']}")
    print(f"Score: {result['score']}")
    print(f"Sentiment: {result['sentiment_label']}")
    print(f"Details: {result['details']}")
