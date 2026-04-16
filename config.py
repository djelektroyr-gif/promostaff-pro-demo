# config.py
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_BASE_DIR = Path(__file__).resolve().parent

# Токен: BotHost/Timeweb часто дублируют одно значение в BOT_TOKEN / TELEGRAM_BOT_TOKEN / TOKEN.
_raw_token = (
    (os.getenv("BOT_TOKEN") or "").strip()
    or (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    or (os.getenv("TOKEN") or "").strip()
)
BOT_TOKEN = _raw_token or None
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения!")


def _parse_admin_ids() -> frozenset[int]:
    ids: set[int] = set()
    raw = (os.getenv("ADMIN_USER_IDS") or "").strip()
    if raw:
        for part in raw.replace(";", ",").split(","):
            p = part.strip()
            if not p:
                continue
            try:
                ids.add(int(p))
            except ValueError:
                continue
    legacy = (os.getenv("ADMIN_USER_ID") or "").strip()
    if legacy:
        try:
            ids.add(int(legacy))
        except ValueError:
            pass
    return frozenset(ids)


ADMIN_USER_IDS: frozenset[int] = _parse_admin_ids()
if not ADMIN_USER_IDS:
    raise ValueError(
        "❌ Задайте ADMIN_USER_IDS (через запятую) или хотя бы ADMIN_USER_ID в переменных окружения!"
    )

# Первый ID для обратной совместимости с кодом, ожидающим один «главный» адресат.
ADMIN_USER_ID: int = min(ADMIN_USER_IDS)


def is_admin_user(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return int(user_id) in ADMIN_USER_IDS


# Настройки проекта
PROJECT_NAME = "Демо-проект PROMOSTAFF"
DEFAULT_RATE = 500  # ставка ₽/час
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"

_fsm_env = (os.getenv("FSM_DB_PATH") or "").strip()
FSM_DB_PATH = str(_BASE_DIR / "promostaff_fsm.db") if not _fsm_env else _fsm_env

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

# Эскалация после пинга просроченных задач (в минутах)
OVERDUE_TASK_ESCALATION_MINUTES = int(os.getenv("OVERDUE_TASK_ESCALATION_MINUTES", "30"))

# Режим разметки по умолчанию для исходящих сообщений бота
PARSE_MODE_TELEGRAM = "MarkdownV2"
