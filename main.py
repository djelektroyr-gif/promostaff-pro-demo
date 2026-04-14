# main.py
import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from db import init_db
from handlers import routers
from services.shift_notifier import run_notifications_once

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Инициализация БД
    init_db()
    logger.info("📁 База данных инициализирована")
    
    # Создаём бота и диспетчер
    bot = Bot(token=BOT_TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Подключаем все роутеры
    for router in routers:
        dp.include_router(router)

    async def _scheduler_loop():
        while True:
            try:
                await run_notifications_once(bot)
            except Exception:
                logger.exception("scheduler loop failed")
            await asyncio.sleep(60)
    asyncio.create_task(_scheduler_loop())
    
    logger.info("🚀 Бот PROMOSTAFF DEMO запущен!")
    
    # Запускаем polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
