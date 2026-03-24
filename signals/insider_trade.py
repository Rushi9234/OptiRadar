import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_insider_trade(ticker: str, insider_df: pd.DataFrame) -> dict:
    result = {
        "signal": "insider_trade",
        "score": 0.0,
        "fired": False,
        "trade_type": "",
        "details": ""
    }

    if insider_df.empty:
        result["details"] = "Insider trade data unavailable"
        return result

    symbol = ticker.replace(".NS", "").upper()

    try:
        matched = insider_df[insider_df['symbol'].str.upper() == symbol]

        if matched.empty:
            result["details"] = f"No insider trades found for {symbol}"
            return result

        cutoff = datetime.now() - timedelta(days=7)
        recent = []

        for _, row in matched.iterrows():
            try:
                date_str = str(row.get('date', '')).strip()
                if not date_str or date_str == 'nan':
                    recent.append(row)
                    continue
                date_part = date_str.split(' ')[0]
                trade_date = datetime.strptime(date_part, "%d-%b-%Y")
                if trade_date >= cutoff:
                    recent.append(row)
            except:
                recent.append(row)

        if not recent:
            result["details"] = f"No insider trades in last 7 days for {symbol}"
            return result

        recent_df = pd.DataFrame(recent)

        buy_keywords = ['market purchase', 'buy', 'acquisition', 'allotment', 'exercise']
        sell_keywords = ['market sale', 'sell', 'disposal', 'transfer']

        trade_type = "unknown"
        score = 0.60

        if 'acqMode' in recent_df.columns:
            mode = str(recent_df.iloc[0]['acqMode']).lower()
            if any(k in mode for k in buy_keywords):
                trade_type = "BUY"
                score = 0.85
            elif any(k in mode for k in sell_keywords):
                trade_type = "SELL"
                score = 0.65
            else:
                trade_type = mode.upper()
                score = 0.60

        if 'personCategory' in recent_df.columns:
            category = str(recent_df.iloc[0]['personCategory']).lower()
            if 'promoter' in category or 'director' in category:
                score = min(score + 0.10, 1.0)

        result["score"] = round(score, 4)
        result["fired"] = True
        result["trade_type"] = trade_type
        result["details"] = (
            f"Insider {trade_type} detected for {symbol} | "
            f"Category: {recent_df.iloc[0].get('personCategory', 'Unknown')} | "
            f"Trades found: {len(recent_df)}"
        )

        logger.info(f"Insider trade detected for {symbol}: {trade_type} score={score}")
        return result

    except Exception as e:
        logger.error(f"Error in insider trade detection for {symbol}: {e}")
        result["details"] = f"Error: {e}"
        return result


if __name__ == "__main__":
    from data.nse_scraper import fetch_insider_trades
    print("Testing insider trade detector...")
    insider_df = fetch_insider_trades()
    print(f"Insider trades fetched: {len(insider_df)} rows")

    if not insider_df.empty:
        test_symbol = insider_df['symbol'].iloc[0]
        test_ticker = test_symbol + ".NS"
        print(f"\nTesting on: {test_ticker}")
        result = detect_insider_trade(test_ticker, insider_df)
        print(f"Signal fired: {result['fired']}")
        print(f"Score: {result['score']}")
        print(f"Trade type: {result['trade_type']}")
        print(f"Details: {result['details']}")
    else:
        print("No insider data to test with")
