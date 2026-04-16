# main.py
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import uuid

from aiogram import Bot, Dispatcher

from config import BOT_TOKEN, FSM_DB_PATH, SCHEDULER_LOCK_NAME, SCHEDULER_LOCK_TTL_SEC, SCHEDULER_POLL_INTERVAL_SEC
from db import init_db, acquire_scheduler_lock
from handlers import routers
from services.sqlite_fsm_storage import SQLiteFSMStorage
from services.shift_notifier import run_notifications_once

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    init_db()
    logger.info("📁 База данных инициализирована")

    bot = Bot(token=BOT_TOKEN)
    storage = SQLiteFSMStorage(FSM_DB_PATH)
    dp = Dispatcher(storage=storage)

    for router in routers:
        dp.include_router(router)

    stop_event = asyncio.Event()
    scheduler_owner = str(uuid.uuid4())

    async def _scheduler_loop() -> None:
        poll = max(10, int(SCHEDULER_POLL_INTERVAL_SEC))
        while not stop_event.is_set():
            try:
                if acquire_scheduler_lock(SCHEDULER_LOCK_NAME, scheduler_owner, SCHEDULER_LOCK_TTL_SEC):
                    await run_notifications_once(bot)
            except Exception:
                logger.exception("scheduler loop failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll)
            except asyncio.TimeoutError:
                continue
        logger.info("Планировщик уведомлений остановлен.")

    sched_task = asyncio.create_task(_scheduler_loop())

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        if not stop_event.is_set():
            logger.info("Получен сигнал остановки, завершаем планировщик…")
        stop_event.set()

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                logger.warning("Сигнал %s не зарегистрирован в event loop", sig)
    else:
        # На Windows add_signal_handler часто недоступен; Ctrl+C всё равно прерывает polling.
        pass

    logger.info("🚀 Бот PROMOSTAFF DEMO запущен!")
    try:
        await dp.start_polling(bot)
    finally:
        stop_event.set()
        sched_task.cancel()
        try:
            await sched_task
        except asyncio.CancelledError:
            pass
        try:
            await bot.session.close()
        except Exception:
            logger.exception("ошибка при закрытии сессии бота")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
