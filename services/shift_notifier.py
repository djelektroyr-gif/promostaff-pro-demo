from __future__ import annotations

import logging
from datetime import datetime, timedelta

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import ADMIN_USER_ID
from db import (
    list_assignments_for_scheduler,
    mark_assignment_event,
    format_date_ru,
)

logger = logging.getLogger(__name__)


def _normalize_time_str(value: str) -> str:
    s = (value or "").strip()
    if len(s) >= 8 and s.count(":") >= 2:
        return s[:5]
    return s


def _shift_start_end(shift_date: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(f"{shift_date} {_normalize_time_str(start_time)}", "%Y-%m-%d %H:%M")
    end = datetime.strptime(f"{shift_date} {_normalize_time_str(end_time)}", "%Y-%m-%d %H:%M")
    if end <= start:
        end += timedelta(days=1)
    return start, end


async def run_notifications_once(bot: Bot) -> None:
    now = datetime.now()
    rows = list_assignments_for_scheduler()
    for r in rows:
        try:
            (
                assignment_id,
                shift_id,
                worker_id,
                status,
                assigned_notify_sent_at,
                reminder_12h_sent_at,
                reminder_3h_sent_at,
                escalation_1h_sent_at,
                checkin_30m_sent_at,
                checkout_30m_sent_at,
                forgot_checkout_sent_at,
                _late_checkin_notified_at,
                _extension_request_minutes,
                _extension_request_status,
                shift_date,
                start_time,
                end_time,
                location,
                _shift_status,
                client_id,
                worker_name,
            ) = r
            dt_start, dt_end = _shift_start_end(str(shift_date), str(start_time), str(end_time))
            to_start = (dt_start - now).total_seconds()
            to_end = (dt_end - now).total_seconds()

            # Мгновенное уведомление о назначении.
            if assigned_notify_sent_at is None:
                await bot.send_message(
                    int(worker_id),
                    "📌 *Вас назначили на смену*\n\n"
                    f"Смена #{shift_id}\n"
                    f"Дата: {format_date_ru(str(shift_date))}\n"
                    f"Время: {start_time}-{end_time}\n"
                    f"Локация: {location}\n\n"
                    "Подтвердите выход в карточке смены.",
                    parse_mode="Markdown",
                )
                mark_assignment_event(int(assignment_id), "assigned_notify_sent_at")

            # Напоминания о подтверждении.
            if status == "pending":
                if reminder_12h_sent_at is None and 0 < to_start <= 12 * 3600:
                    await bot.send_message(
                        int(worker_id),
                        f"⏰ До смены #{shift_id} ~12 часов. Подтвердите выход в боте.",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_12h_sent_at")
                if reminder_3h_sent_at is None and 0 < to_start <= 3 * 3600:
                    await bot.send_message(
                        int(worker_id),
                        f"⏰ До смены #{shift_id} ~3 часа. Срочно подтвердите выход.",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_3h_sent_at")
                if escalation_1h_sent_at is None and 0 < to_start <= 3600:
                    txt = (
                        f"⚠️ Исполнитель {worker_name or worker_id} не подтвердил выход на смену #{shift_id}. "
                        f"Старт: {format_date_ru(str(shift_date))} {start_time}."
                    )
                    await bot.send_message(int(ADMIN_USER_ID), txt)
                    if client_id:
                        await bot.send_message(int(client_id), txt)
                    await bot.send_message(
                        int(worker_id),
                        f"⚠️ До старта смены #{shift_id} меньше часа. Подтвердите выход немедленно.",
                    )
                    mark_assignment_event(int(assignment_id), "escalation_1h_sent_at")

            # Напоминание о чек-ине за 30 минут.
            if status == "confirmed" and checkin_30m_sent_at is None and 0 < to_start <= 1800:
                await bot.send_message(
                    int(worker_id),
                    f"📍 До старта смены #{shift_id} 30 минут. Подготовьтесь отправить чек-ин.",
                )
                mark_assignment_event(int(assignment_id), "checkin_30m_sent_at")

            # Напоминание о чек-ауте за 30 минут до конца.
            if status == "checked_in" and checkout_30m_sent_at is None and 0 < to_end <= 1800:
                await bot.send_message(
                    int(worker_id),
                    f"📸 До завершения смены #{shift_id} 30 минут. Не забудьте чек-аут.",
                )
                mark_assignment_event(int(assignment_id), "checkout_30m_sent_at")

            # Смена должна закончиться, но чек-аута нет.
            if status == "checked_in" and forgot_checkout_sent_at is None and to_end <= 0:
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Закрыть смену", callback_data=f"forgot_close_{shift_id}")],
                        [InlineKeyboardButton(text="⏱ Продлить смену", callback_data=f"forgot_extend_{shift_id}")],
                    ]
                )
                await bot.send_message(
                    int(worker_id),
                    f"⌛ Смена #{shift_id} по времени завершена. Не забыли закрыть смену?",
                    reply_markup=kb,
                )
                msg = (
                    f"⚠️ Исполнитель {worker_name or worker_id} не выполнил чек-аут по смене #{shift_id}. "
                    "Ждём действие (закрыть/продлить)."
                )
                await bot.send_message(int(ADMIN_USER_ID), msg)
                if client_id:
                    await bot.send_message(int(client_id), msg)
                mark_assignment_event(int(assignment_id), "forgot_checkout_sent_at")
        except Exception:
            logger.exception("scheduler row failed shift_id=%s worker_id=%s", r[1], r[2])
