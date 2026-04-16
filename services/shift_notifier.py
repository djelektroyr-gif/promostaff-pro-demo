from __future__ import annotations

import logging
from datetime import datetime, timedelta

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import ADMIN_USER_IDS, OVERDUE_TASK_ESCALATION_MINUTES, PARSE_MODE_TELEGRAM
from db import (
    list_assignments_for_scheduler,
    mark_assignment_event,
    format_date_ru,
    list_due_overdue_task_escalations,
    has_open_tasks_for_worker_on_shift,
    list_open_task_titles_for_worker_on_shift,
    mark_overdue_task_escalated,
)
from services.delivery import send_message_with_retry
from services.text_utils import bold, escape_markdown as em
from services.time_utils import now_local_naive, shift_start_end_local_naive

logger = logging.getLogger(__name__)


def _kb_open_shift(shift_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть смену", callback_data=f"worker_shift_{shift_id}")]
        ]
    )


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
                confirmed_shift_12h_reminder_sent_at,
                confirmed_shift_3h_reminder_sent_at,
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
                assigned_body = (
                    bold("Вас назначили на смену")
                    + "\n\n"
                    + em(f"Смена #{shift_id}")
                    + "\n"
                    + em(f"Дата: {format_date_ru(str(shift_date))}")
                    + "\n"
                    + em(f"Время: {start_time}-{end_time}")
                    + "\n"
                    + em(f"Локация: {location}")
                    + "\n\n"
                    + em("Подтвердите выход в карточке смены.")
                )
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    assigned_body,
                    parse_mode=PARSE_MODE_TELEGRAM,
                    reply_markup=_kb_open_shift(int(shift_id)),
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
                        reply_markup=_kb_open_shift(int(shift_id)),
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
                        reply_markup=_kb_open_shift(int(shift_id)),
                        context=f"rem12_repeat:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_12h_repeat_last_at")
                if escalation_11h_sent_at is None and no_confirm_flagged_at is None and 0 < to_start <= 11 * 3600:
                    txt = (
                        f"⚠️ Исполнитель {worker_name or worker_id} не подтвердил выход на смену #{shift_id} спустя 1 час после 12ч-уведомления. "
                        "Свяжитесь с исполнителем и уточните готовность к выходу."
                    )
                    for aid in ADMIN_USER_IDS:
                        await send_message_with_retry(bot, int(aid), txt, context=f"escal11_admin:{assignment_id}:{aid}")
                    if client_id:
                        await send_message_with_retry(bot, int(client_id), txt, context=f"escal11_client:{assignment_id}")
                    mark_assignment_event(int(assignment_id), "escalation_11h_sent_at")
                    mark_assignment_event(int(assignment_id), "no_confirm_flagged_at")
                if reminder_3h_sent_at is None and 0 < to_start <= 3 * 3600:
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⏰ До смены #{shift_id} ~3 часа. Срочно подтвердите выход.",
                        reply_markup=_kb_open_shift(int(shift_id)),
                        context=f"rem3:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "reminder_3h_sent_at")
                if escalation_1h_sent_at is None and 0 < to_start <= 3600:
                    txt = (
                        f"⚠️ Исполнитель {worker_name or worker_id} не подтвердил выход на смену #{shift_id}. "
                        f"Старт: {format_date_ru(str(shift_date))} {start_time}."
                    )
                    for aid in ADMIN_USER_IDS:
                        await send_message_with_retry(bot, int(aid), txt, context=f"escal1_admin:{assignment_id}:{aid}")
                    if client_id:
                        await send_message_with_retry(bot, int(client_id), txt, context=f"escal1_client:{assignment_id}")
                    await send_message_with_retry(
                        bot,
                        int(worker_id),
                        f"⚠️ До старта смены #{shift_id} меньше часа. Подтвердите выход немедленно.",
                        reply_markup=_kb_open_shift(int(shift_id)),
                        context=f"escal1_worker:{assignment_id}",
                    )
                    mark_assignment_event(int(assignment_id), "escalation_1h_sent_at")

            # Напоминания о смене для уже подтвердивших (не путать с «подтвердите выход» для pending).
            # Окно «~12 ч»: строго больше 3 ч до старта — иначе сработает только блок «~3 ч».
            if (
                status == "confirmed"
                and confirmed_shift_12h_reminder_sent_at is None
                and 3 * 3600 < to_start <= 12 * 3600
            ):
                h, m = int(to_start // 3600), int((to_start % 3600) // 60)
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    bold("Напоминание о смене")
                    + "\n\n"
                    + em(f"До старта смены #{shift_id} осталось около {h}ч {m}м.")
                    + "\n"
                    + em(f"📆 {format_date_ru(str(shift_date))} {start_time}–{end_time}")
                    + "\n"
                    + em(f"📍 {location}")
                    + "\n\n"
                    + em(
                        "Вы уже подтвердили выход. За 30 минут до начала бот напомнит про чек-ин (геолокация и селфи)."
                    ),
                    parse_mode=PARSE_MODE_TELEGRAM,
                    reply_markup=_kb_open_shift(int(shift_id)),
                    context=f"conf_shift_12h:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "confirmed_shift_12h_reminder_sent_at")

            if (
                status == "confirmed"
                and confirmed_shift_3h_reminder_sent_at is None
                and 0 < to_start <= 3 * 3600
            ):
                h, m = int(to_start // 3600), int((to_start % 3600) // 60)
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    bold("Напоминание о смене")
                    + "\n\n"
                    + em(f"До старта смены #{shift_id} осталось около {h}ч {m}м.")
                    + "\n"
                    + em(f"📆 {format_date_ru(str(shift_date))} {start_time}–{end_time}")
                    + "\n"
                    + em(f"📍 {location}")
                    + "\n\n"
                    + em("Скоро откроется чек-ин: геолокация, затем фото. Откройте смену в боте и следуйте шагам."),
                    parse_mode=PARSE_MODE_TELEGRAM,
                    reply_markup=_kb_open_shift(int(shift_id)),
                    context=f"conf_shift_3h:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "confirmed_shift_3h_reminder_sent_at")

            # Напоминание о чек-ине за 30 минут.
            if status == "confirmed" and checkin_30m_sent_at is None and 0 < to_start <= 1800:
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"📍 До старта смены #{shift_id} 30 минут. Откройте смену → «Чек-ин»: сначала геолокация, затем селфи.",
                    reply_markup=_kb_open_shift(int(shift_id)),
                    context=f"checkin30:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "checkin_30m_sent_at")
            if status == "confirmed" and checkin_15m_sent_at is None and 0 < to_start <= 900:
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"⚠️ До старта смены #{shift_id} 15 минут. Выполните чек-ин (гео, затем фото).",
                    reply_markup=_kb_open_shift(int(shift_id)),
                    context=f"checkin15:{assignment_id}",
                )
                mark_assignment_event(int(assignment_id), "checkin_15m_sent_at")
            if status == "confirmed" and no_checkin_start_notified_at is None and to_start <= 0:
                txt = (
                    f"⚠️ По смене #{shift_id} нет чек-ина к старту. "
                    f"Исполнитель {worker_name or worker_id} отмечается как опаздывающий."
                )
                for aid in ADMIN_USER_IDS:
                    await send_message_with_retry(bot, int(aid), txt, context=f"nocheckin_admin:{assignment_id}:{aid}")
                if client_id:
                    await send_message_with_retry(bot, int(client_id), txt, context=f"nocheckin_client:{assignment_id}")
                await send_message_with_retry(
                    bot,
                    int(worker_id),
                    f"⚠️ Смена #{shift_id} уже началась, а чек-ин не выполнен. Это фиксируется как опоздание.",
                    reply_markup=_kb_open_shift(int(shift_id)),
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
                    f"📸 До завершения смены #{shift_id} 30 минут. Чек-аут: геолокация, затем селфи.",
                    reply_markup=_kb_open_shift(int(shift_id)),
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
                for aid in ADMIN_USER_IDS:
                    await send_message_with_retry(bot, int(aid), msg, context=f"forgot_checkout_admin:{assignment_id}:{aid}")
                if client_id:
                    await send_message_with_retry(bot, int(client_id), msg, context=f"forgot_checkout_client:{assignment_id}")
                mark_assignment_event(int(assignment_id), "forgot_checkout_sent_at")
        except Exception:
            logger.exception("scheduler row failed shift_id=%s worker_id=%s", r[1], r[2])

    # Эскалация по просроченным задачам после массового пинга.
    try:
        for ping_row in list_due_overdue_task_escalations(wait_minutes=OVERDUE_TASK_ESCALATION_MINUTES):
            try:
                ping_id, shift_id, worker_id, _ping_sent_at, client_id, worker_name = ping_row
                if not has_open_tasks_for_worker_on_shift(int(shift_id), int(worker_id)):
                    mark_overdue_task_escalated(int(ping_id))
                    continue

                titles = list_open_task_titles_for_worker_on_shift(int(shift_id), int(worker_id), limit=5)
                preview = "\n".join([f"• {t}" for t in titles]) if titles else "• (список задач недоступен)"
                text = (
                    f"🚨 После пинга просроченные задачи всё ещё не закрыты.\n\n"
                    f"Смена: #{shift_id}\n"
                    f"Исполнитель: {worker_name or worker_id}\n"
                    f"Открытые задачи:\n{preview}\n\n"
                    "Рекомендуем связаться с исполнителем или назначить замену."
                )
                for aid in ADMIN_USER_IDS:
                    await send_message_with_retry(
                        bot,
                        int(aid),
                        text,
                        context=f"overdue_escal_admin:{ping_id}:{aid}",
                    )
                if client_id:
                    await send_message_with_retry(
                        bot,
                        int(client_id),
                        text,
                        context=f"overdue_escal_client:{ping_id}",
                    )
                mark_overdue_task_escalated(int(ping_id))
            except Exception:
                logger.exception("overdue escalation failed ping_id=%s", ping_row[0] if ping_row else None)
    except Exception:
        logger.exception("overdue escalation batch failed")
