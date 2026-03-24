import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detect_volume_spike(df: pd.DataFrame) -> dict:
    """
    Detect if today's volume is unusually high compared to 30-day average.
    Score = min(today_volume / 30d_avg_volume / 3, 1.0)
    A 3x spike = score of 1.0 (maximum)
    """
    result = {
        "signal": "volume_spike",
        "score": 0.0,
        "fired": False,
        "details": ""
    }

    if df.empty or len(df) < 5:
        result["details"] = "Insufficient data"
        return result

    try:
        # 30-day rolling average volume (excluding today)
        avg_volume = df['volume'].iloc[:-1].tail(30).mean()
        today_volume = df['volume'].iloc[-1]

        if avg_volume == 0:
            result["details"] = "Average volume is zero"
            return result

        ratio = today_volume / avg_volume
        score = min(ratio / 3.0, 1.0)

        result["score"] = round(score, 4)
        result["fired"] = score >= 0.4  # fires if volume is at least 1.2x average
        result["details"] = (
            f"Today volume: {int(today_volume):,} | "
            f"30d avg: {int(avg_volume):,} | "
            f"Ratio: {ratio:.2f}x"
        )

        logger.info(f"Volume spike score: {score:.4f} | Ratio: {ratio:.2f}x")
        return result

    except Exception as e:
        logger.error(f"Error in volume spike detection: {e}")
        result["details"] = f"Error: {e}"
        return result


if __name__ == "__main__":
    from data.fetcher import fetch_stock_data
    print("Testing volume spike detector on RELIANCE.NS...")
    df = fetch_stock_data("RELIANCE.NS")
    result = detect_volume_spike(df)
    print(f"Signal fired: {result['fired']}")
    print(f"Score: {result['score']}")
    print(f"Details: {result['details']}")