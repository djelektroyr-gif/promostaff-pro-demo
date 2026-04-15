# main.py
import asyncio
import logging
import uuid
from aiogram import Bot, Dispatcher

from config import BOT_TOKEN, FSM_DB_PATH, SCHEDULER_LOCK_NAME, SCHEDULER_LOCK_TTL_SEC, SCHEDULER_POLL_INTERVAL_SEC
from db import init_db, acquire_scheduler_lock
from handlers import routers
from services.sqlite_fsm_storage import SQLiteFSMStorage
from services.shift_notifier import run_notifications_once

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Инициализация БД
    init_db()
    logger.info("📁 База данных инициализирована")
    
    # Создаём бота и диспетчер
    bot = Bot(token=BOT_TOKEN)
    storage = SQLiteFSMStorage(FSM_DB_PATH)
    dp = Dispatcher(storage=storage)
    
    # Подключаем все роутеры
    for router in routers:
        dp.include_router(router)

    scheduler_owner = str(uuid.uuid4())
    async def _scheduler_loop():
        while True:
            try:
                if acquire_scheduler_lock(SCHEDULER_LOCK_NAME, scheduler_owner, SCHEDULER_LOCK_TTL_SEC):
                    await run_notifications_once(bot)
            except Exception:
                logger.exception("scheduler loop failed")
            await asyncio.sleep(max(10, int(SCHEDULER_POLL_INTERVAL_SEC)))
    asyncio.create_task(_scheduler_loop())
    
    logger.info("🚀 Бот PROMOSTAFF DEMO запущен!")
    
    # Запускаем polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
