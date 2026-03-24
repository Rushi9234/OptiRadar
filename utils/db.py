import sqlite3
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "optiradar.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            score REAL NOT NULL,
            bucket TEXT NOT NULL,
            signals_fired TEXT,
            price_at_signal REAL,
            bulk_deal_score REAL,
            volume_score REAL,
            insider_score REAL,
            technical_score REAL,
            sentiment_score REAL,
            ai_card TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            ticker TEXT NOT NULL,
            price_at_signal REAL,
            price_1d REAL,
            price_3d REAL,
            price_5d REAL,
            return_1d REAL,
            return_3d REAL,
            return_5d REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (signal_id) REFERENCES signals(id)
        )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")


def save_signal(result: dict, ai_card: str = "") -> int:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO signals (
            ticker, score, bucket, signals_fired, price_at_signal,
            bulk_deal_score, volume_score, insider_score,
            technical_score, sentiment_score, ai_card, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["ticker"],
        result["score"],
        result["bucket"],
        ", ".join(result.get("signals_fired", [])),
        result.get("current_price", 0),
        result["bulk_deal"]["score"],
        result["volume_spike"]["score"],
        result["insider_trade"]["score"],
        result["technical"]["score"],
        result["sentiment"]["score"],
        ai_card,
        datetime.now().isoformat()
    ))

    signal_id = cursor.lastrowid
    conn.commit()
    conn.close()
    logger.info(f"Signal saved for {result['ticker']} with id {signal_id}")
    return signal_id


def get_recent_signals(limit: int = 50) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, score, bucket, signals_fired, price_at_signal, ai_card, created_at
        FROM signals
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "ticker": r[0],
            "score": r[1],
            "bucket": r[2],
            "signals_fired": r[3],
            "price_at_signal": r[4],
            "ai_card": r[5],
            "created_at": r[6]
        }
        for r in rows
    ]


def get_hot_signals(limit: int = 20) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, score, bucket, signals_fired, price_at_signal, ai_card, created_at
        FROM signals
        WHERE bucket IN ('Hot', 'Very Hot')
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "ticker": r[0],
            "score": r[1],
            "bucket": r[2],
            "signals_fired": r[3],
            "price_at_signal": r[4],
            "ai_card": r[5],
            "created_at": r[6]
        }
        for r in rows
    ]


def save_price_outcome(signal_id: int, ticker: str, price_at_signal: float,
                        price_1d: float = 0, price_3d: float = 0, price_5d: float = 0):
    conn = get_connection()
    cursor = conn.cursor()

    def safe_return(p_now, p_then):
        if p_then and p_then > 0:
            return round((p_now - p_then) / p_then * 100, 2)
        return 0.0

    cursor.execute("""
        INSERT INTO price_outcomes (
            signal_id, ticker, price_at_signal,
            price_1d, price_3d, price_5d,
            return_1d, return_3d, return_5d
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        signal_id, ticker, price_at_signal,
        price_1d, price_3d, price_5d,
        safe_return(price_1d, price_at_signal),
        safe_return(price_3d, price_at_signal),
        safe_return(price_5d, price_at_signal)
    ))

    conn.commit()
    conn.close()
    logger.info(f"Price outcome saved for signal {signal_id}")


def get_signal_accuracy() -> dict:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN return_1d > 0 THEN 1 ELSE 0 END) as wins_1d,
            SUM(CASE WHEN return_3d > 0 THEN 1 ELSE 0 END) as wins_3d,
            SUM(CASE WHEN return_5d > 5 THEN 1 ELSE 0 END) as big_moves_5d,
            AVG(return_1d) as avg_return_1d,
            AVG(return_3d) as avg_return_3d,
            AVG(return_5d) as avg_return_5d
        FROM price_outcomes
    """)

    row = cursor.fetchone()
    conn.close()

    if not row or row[0] == 0:
        return {"total": 0, "message": "No outcomes tracked yet"}

    total = row[0]
    return {
        "total_signals_tracked": total,
        "win_rate_1d": round(row[1] / total * 100, 1),
        "win_rate_3d": round(row[2] / total * 100, 1),
        "big_moves_5d_pct": round(row[3] / total * 100, 1),
        "avg_return_1d": round(row[4], 2),
        "avg_return_3d": round(row[5], 2),
        "avg_return_5d": round(row[6], 2)
    }


if __name__ == "__main__":
    init_db()
    print(f"Database created at: {DB_PATH}")
    print("Tables: signals, price_outcomes")
    print("Testing save_signal...")

    dummy_result = {
        "ticker": "RELIANCE.NS",
        "score": 72.5,
        "bucket": "Hot",
        "signals_fired": ["Technical (0.80)", "Volume Spike (0.60)"],
        "current_price": 1414.8,
        "bulk_deal": {"score": 0.0},
        "volume_spike": {"score": 0.60},
        "insider_trade": {"score": 0.0},
        "technical": {"score": 0.80},
        "sentiment": {"score": 0.10},
    }

    signal_id = save_signal(dummy_result, ai_card="Test alert card.")
    print(f"Signal saved with ID: {signal_id}")

    recent = get_recent_signals(5)
    print(f"Recent signals in DB: {len(recent)}")
    print(recent[0])