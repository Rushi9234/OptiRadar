import os
import logging
import asyncio
from telegram import Bot
from telegram.error import TelegramError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


async def send_message_async(text: str):
    bot = Bot(token=BOT_TOKEN)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=text,
        parse_mode="HTML"
    )


def send_alert(text: str):
    try:
        asyncio.run(send_message_async(text))
        logger.info("Telegram message sent")
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")


def format_signal_alert(result: dict, ai_card: str) -> str:
    ticker = result["ticker"].replace(".NS", "")
    score = result["score"]
    bucket = result["bucket"]
    price = result.get("current_price", 0)
    signals = result.get("signals_fired", [])

    bucket_emoji = {
        "Very Hot": "🔥🔥",
        "Hot": "🔥",
        "Warm": "⚡",
        "Cold": "❄️"
    }.get(bucket, "📊")

    message = f"""{bucket_emoji} <b>OptiRadar Alert — {ticker}</b>

💰 Price: ₹{price}
📊 Score: {score}/100
🎯 Bucket: {bucket}
⚡ Signals: {', '.join(signals) if signals else 'None'}

🤖 <b>AI Analysis:</b>
{ai_card}

<i>OptiRadar — NSE Signal Radar</i>"""

    return message


def format_digest(digest_text: str) -> str:
    return f"""📋 <b>OptiRadar Daily Digest</b>

{digest_text}

<i>OptiRadar — End of Day Summary</i>"""


if __name__ == "__main__":
    print("To set up Telegram:")
    print("1. Open Telegram and search @BotFather")
    print("2. Send /newbot and follow steps")
    print("3. Copy the token into .env as TELEGRAM_BOT_TOKEN")
    print("4. Search @userinfobot in Telegram")
    print("5. Send it any message — it replies with your Chat ID")
    print("6. Copy that ID into .env as TELEGRAM_CHAT_ID")
    print("")

    if BOT_TOKEN and CHAT_ID:
        print("Sending test message...")
        send_alert("🚀 <b>OptiRadar Bot is live!</b>\n\nYour NSE signal radar is connected and ready.")
        print("Check your Telegram!")
    else:
        print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing in .env")