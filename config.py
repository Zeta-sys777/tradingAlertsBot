import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Telegram
TG_TOKEN = os.getenv("TG_TOKEN")
TELEGRAM_CHANNEL_ID = int(os.getenv("TELEGRAM_CHANNEL_ID", "0"))

if not TG_TOKEN:
    raise ValueError("TG_TOKEN не задан в .env")

if TELEGRAM_CHANNEL_ID == 0:
    raise ValueError("TELEGRAM_CHANNEL_ID не задан в .env")

# BingX
BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET")
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com")

if not BINGX_API_KEY or not BINGX_API_SECRET:
    raise ValueError("BINGX_API_KEY или BINGX_API_SECRET не заданы в .env")
