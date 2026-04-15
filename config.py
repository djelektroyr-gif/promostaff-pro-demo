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
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
FSM_DB_PATH = os.getenv("FSM_DB_PATH", "promostaff_fsm.db").strip() or "promostaff_fsm.db"

# Планировщик
SCHEDULER_POLL_INTERVAL_SEC = int(os.getenv("SCHEDULER_POLL_INTERVAL_SEC", "60"))
SCHEDULER_LOCK_NAME = os.getenv("SCHEDULER_LOCK_NAME", "shift_notifier").strip() or "shift_notifier"
SCHEDULER_LOCK_TTL_SEC = int(os.getenv("SCHEDULER_LOCK_TTL_SEC", "90"))

# Надёжная доставка уведомлений
NOTIFY_RETRY_ATTEMPTS = int(os.getenv("NOTIFY_RETRY_ATTEMPTS", "3"))
NOTIFY_RETRY_BASE_DELAY_SEC = float(os.getenv("NOTIFY_RETRY_BASE_DELAY_SEC", "0.8"))

# Оплата/штрафы в DEMO
LATE_CHECKIN_PENALTY_FIRST_30M_HOURS = float(
    os.getenv("LATE_CHECKIN_PENALTY_FIRST_30M_HOURS", "1.0")
)
# none | quarter_up | half_up | hour_up
BILLING_ROUNDING_MODE = (os.getenv("BILLING_ROUNDING_MODE", "none") or "none").strip().lower()

# Время напоминаний (в часах до начала смены)
REMINDER_12H = 12
REMINDER_3H = 3
