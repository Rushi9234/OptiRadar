import pandas as pd
import ta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_technical(df: pd.DataFrame) -> dict:
    result = {
        "signal": "technical",
        "score": 0.0,
        "fired": False,
        "patterns": [],
        "details": ""
    }

    if df.empty or len(df) < 20:
        result["details"] = "Insufficient data for technical analysis (need 20+ days)"
        return result

    try:
        close = df["close"]
        patterns = []
        scores = []

        rsi_indicator = ta.momentum.RSIIndicator(close=close, window=14)
        rsi = rsi_indicator.rsi()
        current_rsi = rsi.iloc[-1]
        prev_rsi = rsi.iloc[-2] if len(rsi) > 1 else current_rsi

        if current_rsi < 35:
            rsi_score = round((35 - current_rsi) / 35, 4)
            patterns.append(f"RSI Oversold ({current_rsi:.1f})")
            scores.append(min(rsi_score, 0.90))
        elif current_rsi > 65:
            rsi_score = round((current_rsi - 65) / 35, 4)
            patterns.append(f"RSI Overbought ({current_rsi:.1f})")
            scores.append(min(rsi_score, 0.75))
        elif prev_rsi < 50 and current_rsi >= 50:
            patterns.append(f"RSI Bullish Cross 50 ({current_rsi:.1f})")
            scores.append(0.60)

        macd_indicator = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        macd_hist = macd_indicator.macd_diff()

        if len(macd_hist) > 1:
            curr_hist = macd_hist.iloc[-1]
            prev_hist = macd_hist.iloc[-2]
            if prev_hist < 0 and curr_hist >= 0:
                patterns.append("MACD Bullish Crossover")
                scores.append(0.80)
            elif prev_hist > 0 and curr_hist <= 0:
                patterns.append("MACD Bearish Crossover")
                scores.append(0.65)
            elif curr_hist > 0 and curr_hist > prev_hist:
                patterns.append("MACD Momentum Building")
                scores.append(0.55)

        bb = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        bb_upper = bb.bollinger_hband()
        bb_lower = bb.bollinger_lband()
        bb_mid = bb.bollinger_mavg()

        current_close = close.iloc[-1]
        current_upper = bb_upper.iloc[-1]
        current_lower = bb_lower.iloc[-1]
        current_mid = bb_mid.iloc[-1]

        if current_close <= current_lower * 1.02:
            patterns.append("BB Lower Band Touch (Oversold)")
            scores.append(0.75)
        elif current_close >= current_upper * 0.98:
            patterns.append("BB Upper Band Breakout")
            scores.append(0.70)

        band_width = (current_upper - current_lower) / current_mid
        avg_bandwidth = ((bb_upper - bb_lower) / bb_mid).tail(20).mean()
        if band_width < avg_bandwidth * 0.7:
            patterns.append("BB Squeeze (Volatility Compression)")
            scores.append(0.65)

        if not scores:
            result["details"] = f"No strong technical pattern | RSI: {current_rsi:.1f} | Close: {current_close:.2f}"
            return result

        base_score = max(scores)
        if len(patterns) >= 2:
            base_score = min(base_score + 0.10, 1.0)
        if len(patterns) >= 3:
            base_score = min(base_score + 0.05, 1.0)

        result["score"] = round(base_score, 4)
        result["fired"] = base_score >= 0.50
        result["patterns"] = patterns
        result["details"] = f"Patterns: {', '.join(patterns)} | RSI: {current_rsi:.1f} | Score: {base_score:.4f}"

        logger.info(f"Technical score: {base_score:.4f} | Patterns: {patterns}")
        return result

    except Exception as e:
        logger.error(f"Error in technical detection: {e}")
        result["details"] = f"Error: {e}"
        return result


if __name__ == "__main__":
    from data.fetcher import fetch_stock_data
    print("Testing technical detector on RELIANCE.NS...")
    df = fetch_stock_data("RELIANCE.NS")
    result = detect_technical(df)
    print(f"Signal fired: {result['fired']}")
    print(f"Score: {result['score']}")
    print(f"Patterns: {result['patterns']}")
    print(f"Details: {result['details']}")
