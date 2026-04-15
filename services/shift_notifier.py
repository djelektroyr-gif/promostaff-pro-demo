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
from services.delivery import send_message_with_retry
from services.time_utils import now_local_naive, shift_start_end_local_naive

logger = logging.getLogger(__name__)


def _normalize_time_str(value: str) -> str:
    s = (value or "").strip()
    if len(s) >= 8 and s.count(":") >= 2:
        return s[:5]
    return s


def _shift_start_end(shift_date: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    return shift_start_end_local_naive(shift_date, start_time, end_time)


def _to_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).replace(" ", "T", 1) if "T" not in str(value) else str(value)
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


async def run_notifications_once(bot: Bot) -> None:
    now = now_local_naive()
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
                reminder_12h_repeat_last_at,
                reminder_3h_sent_at,
                escalation_11h_sent_at,
                escalation_1h_sent_at,
                checkin_30m_sent_at,
                checkin_15m_sent_at,
                checkout_30m_sent_at,
                forgot_checkout_sent_at,
                late_checkin_notified_at,
                no_confirm_flagged_at,
                no_checkin_start_notified_at,
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
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    "📌 *Вас назначили на смену*\n\n"
                    f"Смена #{shift_id}\n"
                    f"Дата: {format_date_ru(str(shift_date))}\n"
                    f"Время: {start_time}-{end_time}\n"
                    f"Локация: {location}\n\n"
                    "Подтвердите выход в карточке смены.",
                    parse_mode="Markdown",
                    context=f"assigned:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "assigned_notify_sent_at")

            # Напоминания о подтверждении.
            if status == "pending":
                if reminder_12h_sent_at is None and 0 < to_start <= 12 * 3600:
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⏰ До смены #{shift_id} ~12 часов. Подтвердите выход в боте.",
                        context=f"rem12:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_12h_sent_at")
                    mark_assignment_event(int(assignment_id), "reminder_12h_repeat_last_at")
                # В течение первого часа после 12ч-напоминания повторяем каждые 15 минут.
                if (
                    reminder_12h_sent_at is not None
                    and 11 * 3600 < to_start <= 12 * 3600
                    and (
                        reminder_12h_repeat_last_at is None
                        or (_to_dt(reminder_12h_repeat_last_at) is None)
                        or (now - _to_dt(reminder_12h_repeat_last_at)).total_seconds() >= 15 * 60
                    )
                ):
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⏰ Напоминание: смена #{shift_id} через ~{int(to_start // 3600)}ч {int((to_start % 3600) // 60)}м. Подтвердите выход.",
                        context=f"rem12_repeat:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_12h_repeat_last_at")
                if escalation_11h_sent_at is None and no_confirm_flagged_at is None and 0 < to_start <= 11 * 3600:
                    txt = (
                        f"⚠️ Исполнитель {worker_name or worker_id} не подтвердил выход на смену #{shift_id} спустя 1 час после 12ч-уведомления. "
                        "Свяжитесь с исполнителем и уточните готовность к выходу."
                    )
                    await send_message_with_retry(bot, int(ADMIN_USER_ID), txt, context=f"escal11_admin:{assignment_id}")
                    if client_id:
                        await send_message_with_retry(bot, int(client_id), txt, context=f"escal11_client:{assignment_id}")
                    mark_assignment_event(int(assignment_id), "escalation_11h_sent_at")
                    mark_assignment_event(int(assignment_id), "no_confirm_flagged_at")
                if reminder_3h_sent_at is None and 0 < to_start <= 3 * 3600:
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⏰ До смены #{shift_id} ~3 часа. Срочно подтвердите выход.",
                        context=f"rem3:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_3h_sent_at")
                if escalation_1h_sent_at is None and 0 < to_start <= 3600:
                    txt = (
                        f"⚠️ Исполнитель {worker_name or worker_id} не подтвердил выход на смену #{shift_id}. "
                        f"Старт: {format_date_ru(str(shift_date))} {start_time}."
                    )
                    await send_message_with_retry(bot, int(ADMIN_USER_ID), txt, context=f"escal1_admin:{assignment_id}")
                    if client_id:
                        await send_message_with_retry(bot, int(client_id), txt, context=f"escal1_client:{assignment_id}")
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⚠️ До старта смены #{shift_id} меньше часа. Подтвердите выход немедленно.",
                        context=f"escal1_worker:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "escalation_1h_sent_at")

            # Напоминание о чек-ине за 30 минут.
            if status == "confirmed" and checkin_30m_sent_at is None and 0 < to_start <= 1800:
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"📍 До старта смены #{shift_id} 30 минут. Пришлите геолокацию и селфи на площадке (чек-ин).",
                    context=f"checkin30:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "checkin_30m_sent_at")
            if status == "confirmed" and checkin_15m_sent_at is None and 0 < to_start <= 900:
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"⚠️ До старта смены #{shift_id} 15 минут. Если вы на площадке — выполните чек-ин сейчас.",
                    context=f"checkin15:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "checkin_15m_sent_at")
            if status == "confirmed" and no_checkin_start_notified_at is None and to_start <= 0:
                txt = (
                    f"⚠️ По смене #{shift_id} нет чек-ина к старту. "
                    f"Исполнитель {worker_name or worker_id} отмечается как опаздывающий."
                )
                await send_message_with_retry(bot, int(ADMIN_USER_ID), txt, context=f"nocheckin_admin:{assignment_id}")
                if client_id:
                    await send_message_with_retry(bot, int(client_id), txt, context=f"nocheckin_client:{assignment_id}")
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"⚠️ Смена #{shift_id} уже началась, а чек-ин не выполнен. Это фиксируется как опоздание.",
                    context=f"nocheckin_worker:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "no_checkin_start_notified_at")
                if late_checkin_notified_at is None:
                    mark_assignment_event(int(assignment_id), "late_checkin_notified_at")

            # Напоминание о чек-ауте за 30 минут до конца.
            if status == "checked_in" and checkout_30m_sent_at is None and 0 < to_end <= 1800:
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"📸 До завершения смены #{shift_id} 30 минут. Не забудьте чек-аут.",
                    context=f"checkout30:{assignment_id}",
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
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"⌛ Смена #{shift_id} по времени завершена. Не забыли закрыть смену?",
                    reply_markup=kb,
                    context=f"forgot_checkout_worker:{assignment_id}",
                )
                msg = (
                    f"⚠️ Исполнитель {worker_name or worker_id} не выполнил чек-аут по смене #{shift_id}. "
                    "Ждём действие (закрыть/продлить)."
                )
                await send_message_with_retry(bot, int(ADMIN_USER_ID), msg, context=f"forgot_checkout_admin:{assignment_id}")
                if client_id:
                    await send_message_with_retry(bot, int(client_id), msg, context=f"forgot_checkout_client:{assignment_id}")
                mark_assignment_event(int(assignment_id), "forgot_checkout_sent_at")
        except Exception:
            logger.exception("scheduler row failed shift_id=%s worker_id=%s", r[1], r[2])
