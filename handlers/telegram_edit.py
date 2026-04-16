# handlers/telegram_edit.py — безопасное редактирование сообщений из callback
from __future__ import annotations

import logging

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


async def safe_edit_or_resend(
    callback: CallbackQuery,
    text: str,
    *,
    parse_mode: str | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
    disable_web_page_preview: bool | None = None,
) -> None:
    """Редактирует текст или подпись к фото; при ошибке типа сообщения — удаляет и шлёт новое."""
    msg = callback.message
    if not msg:
        await callback.answer()
        return
    try:
        if msg.photo:
            await msg.edit_caption(
                caption=(text or "")[:1024],
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
        else:
            await msg.edit_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
    except TelegramBadRequest as e:
        err = (e.message or str(e)).lower()
        if "message is not modified" in err:
            await callback.answer()
            return
        logger.warning(
            "safe_edit_or_resend: edit failed mid=%s, resend: %s",
            getattr(msg, "message_id", None),
            e,
        )
        try:
            await msg.delete()
        except TelegramBadRequest:
            pass
        await msg.answer(
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
