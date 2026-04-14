# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Токен берётся из переменных окружения (Bothost / Timeweb)
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения!")

# ID администратора (вы)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
if ADMIN_USER_ID == 0:
    raise ValueError("❌ ADMIN_USER_ID не установлен в переменных окружения!")

# Настройки проекта
PROJECT_NAME = "Демо-проект PROMOSTAFF"
DEFAULT_RATE = 500  # ставка ₽/час
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Время напоминаний (в часах до начала смены)
REMINDER_12H = 12
REMINDER_3H = 3
