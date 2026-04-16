# services/admin_broadcast.py — рассылка всем администраторам
from __future__ import annotations

import logging
from typing import Any

from aiogram import Bot

from config import ADMIN_USER_IDS

logger = logging.getLogger(__name__)


async def send_all_admins(bot: Bot, text: str, **kwargs: Any) -> None:
    for aid in ADMIN_USER_IDS:
        try:
            await bot.send_message(int(aid), text, **kwargs)
        except Exception as e:
            logger.warning(
                "Не удалось отправить сообщение администратору aid=%s: %s",
                aid,
                e,
                exc_info=True,
            )
