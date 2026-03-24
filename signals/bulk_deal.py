import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detect_bulk_deal(ticker: str, bulk_df: pd.DataFrame) -> dict:
    """
    Detect if a stock has a bulk/block deal today.
    Score = 0.85 if deal found with large quantity, 0.6 if deal found with small quantity.
    """
    result = {
        "signal": "bulk_deal",
        "score": 0.0,
        "fired": False,
        "details": ""
    }

    symbol = ticker.replace(".NS", "").upper()

    # If NSE bulk deal API failed, return a neutral result gracefully
    if bulk_df.empty:
        result["details"] = "Bulk deal data unavailable (NSE API)"
        return result

    try:
        matched = bulk_df[bulk_df['symbol'].str.upper() == symbol]

        if matched.empty:
            result["details"] = f"No bulk deal found for {symbol} today"
            return result

        row = matched.iloc[0]

        # Try to extract quantity
        quantity = 0
        for col in ['quantity', 'bdQty', 'qty']:
            if col in row.index:
                try:
                    quantity = float(str(row[col]).replace(',', ''))
                    break
                except:
                    pass

        # Score based on size of deal
        if quantity >= 5_000_000:       # 50 lakh+ shares = very large
            score = 0.95
        elif quantity >= 1_000_000:     # 10 lakh+ shares = large
            score = 0.85
        elif quantity >= 100_000:       # 1 lakh+ shares = medium
            score = 0.70
        else:
            score = 0.60                # Small deal still counts

        result["score"] = score
        result["fired"] = True
        result["details"] = (
            f"Bulk deal found for {symbol} | "
            f"Quantity: {int(quantity):,} | "
            f"Score: {score}"
        )

        logger.info(f"Bulk deal detected for {symbol}: score={score}")
        return result

    except Exception as e:
        logger.error(f"Error in bulk deal detection for {symbol}: {e}")
        result["details"] = f"Error: {e}"
        return result


if __name__ == "__main__":
    from data.nse_scraper import fetch_bulk_deals
    print("Testing bulk deal detector...")
    bulk_df = fetch_bulk_deals()
    print(f"Bulk deals fetched: {len(bulk_df)} rows")

    # Test with a dummy deal to verify scoring logic works
    test_df = pd.DataFrame([{
        "symbol": "RELIANCE",
        "quantity": "2000000"
    }])
    result = detect_bulk_deal("RELIANCE.NS", test_df)
    print(f"\nTest with dummy data:")
    print(f"Signal fired: {result['fired']}")
    print(f"Score: {result['score']}")
    print(f"Details: {result['details']}")

    # Test with real data if available
    if not bulk_df.empty:
        ticker_to_test = bulk_df['symbol'].iloc[0] + ".NS"
        result2 = detect_bulk_deal(ticker_to_test, bulk_df)
        print(f"\nReal data test ({ticker_to_test}):")
        print(f"Signal fired: {result2['fired']}")
        print(f"Score: {result2['score']}")
        print(f"Details: {result2['details']}")