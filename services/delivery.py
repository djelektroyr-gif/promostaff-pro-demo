from __future__ import annotations

import asyncio
import logging
from typing import Any

from aiogram import Bot

from config import NOTIFY_RETRY_ATTEMPTS, NOTIFY_RETRY_BASE_DELAY_SEC
from db import record_notification_failure

logger = logging.getLogger(__name__)


async def send_message_with_retry(
    bot: Bot,
    chat_id: int,
    text: str,
    *,
    context: str,
    **kwargs: Any,
) -> bool:
    attempts = max(1, int(NOTIFY_RETRY_ATTEMPTS))
    base_delay = max(0.1, float(NOTIFY_RETRY_BASE_DELAY_SEC))
    last_err = ""
    for i in range(attempts):
        try:
            await bot.send_message(chat_id=int(chat_id), text=text, **kwargs)
            return True
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if i < attempts - 1:
                await asyncio.sleep(base_delay * (2**i))
    logger.warning("notify failed context=%s chat_id=%s err=%s", context, chat_id, last_err)
    try:
        record_notification_failure(int(chat_id), context, text[:1000], last_err[:500], attempts)
    except Exception:
        logger.exception("record_notification_failure failed")
    return False
