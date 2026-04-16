# handlers/shifts.py
import logging
import math
from datetime import datetime
from datetime import timedelta

from aiogram import Router, F, types, Bot
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import (
    get_client,
    get_worker,
    get_shift,
    get_shift_assignments,
    get_workers_assignable,
    list_open_shifts_admin,
    list_unconfirmed_assignments,
    replace_assignment_worker,
    log_shift_replacement,
    get_assignment,
    confirm_assignment,
    do_checkin,
    do_checkout,
    get_shift_report,
    get_shift_breaks,
    get_worker_break_stats,
    get_active_break,
    start_assignment_break,
    stop_assignment_break,
    record_overdue_task_ping,
    list_shifts_for_client,
    list_shifts_for_worker,
    client_owns_shift,
    list_projects_for_client,
    get_shift_with_owner,
    mark_assignment_event_by_shift_worker,
    set_extension_request,
    resolve_extension_request,
    extend_shift_end_time,
    set_assignment_checkin_geo_failed,
    format_date_ru,
    auto_close_expired_breaks,
)
from config import PARSE_MODE_TELEGRAM, is_admin_user
from services.admin_broadcast import send_all_admins
from services.text_utils import bold, escape_markdown as em
from services.time_utils import now_local_naive, shift_start_end_local_naive
from .telegram_edit import safe_edit_or_resend
from states import CheckinFlow, CheckoutFlow, ShiftExtensionFlow, ClientMessageWorkerFlow

router = Router()
logger = logging.getLogger(__name__)

# Статус в JOIN-строке assignments + workers всегда a[3]; ФИО — предпоследнее поле (a[-2]).
A_STATUS = 3


def _assign_worker_name(row: tuple) -> str:
    if len(row) >= 2 and row[-2]:
        return str(row[-2])
    return str(row[2]) if len(row) > 2 else "—"


def _distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine distance in meters."""
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _shift_geo_limits(shift: tuple | None) -> tuple[float | None, float | None, int]:
    if not shift or len(shift) < 12:
        return None, None, 300

    def _to_float(v):
        try:
            return float(v) if v is not None and str(v).strip() != "" else None
        except Exception:
            return None

    def _to_radius(v):
        try:
            r = int(float(v))
        except Exception:
            return None
        return r if r > 0 else None

    # Поддерживаем обе схемы:
    # 1) старая SQLite с миграциями: [..., created_at, expected_lat, expected_lng, checkin_radius_m]
    # 2) новая/чистая:            [..., expected_lat, expected_lng, checkin_radius_m, created_at]
    variants = (
        (shift[8], shift[9], shift[10]),   # new order
        (shift[9], shift[10], shift[11]),  # migrated old order
    )
    for lat_raw, lng_raw, radius_raw in variants:
        lat = _to_float(lat_raw)
        lng = _to_float(lng_raw)
        radius = _to_radius(radius_raw)
        if lat is not None and lng is not None and radius is not None:
            return lat, lng, radius

    # Фолбэк: гео-ограждение выключено, но радиус пробуем сохранить.
    radius = _to_radius(shift[10]) or _to_radius(shift[11]) or 300
    return None, None, radius


def _shift_start(shift: tuple) -> datetime:
    # Поддерживаем оба формата:
    # - get_shift():            (id, project_id, shift_date, start_time, end_time, ...)
    # - get_shift_with_owner(): (id, shift_date, start_time, end_time, ...)
    candidates = (
        (1, 2, 3),  # get_shift_with_owner
        (2, 3, 4),  # get_shift
    )
    for d_idx, s_idx, e_idx in candidates:
        if len(shift) <= e_idx:
            continue
        shift_date = str(shift[d_idx] or "").strip()
        start_time = str(shift[s_idx] or "").strip()
        end_time = str(shift[e_idx] or "").strip()
        if not shift_date or not start_time or not end_time:
            continue
        if "-" not in shift_date:
            continue
        try:
            start, _ = shift_start_end_local_naive(shift_date, start_time, end_time)
            return start
        except Exception:
            continue
    raise ValueError(f"cannot parse shift start from row: {shift!r}")


def _assignment_status_line(a: tuple) -> str:
    status = str(a[A_STATUS] or "")
    confirmed_at = str(a[4] or "—")
    if status == "pending":
        return f"⏳ Не подтвердил (подтверждение: {confirmed_at})"
    if status == "confirmed":
        return f"✅ Подтвердил ({confirmed_at})"
    if status == "checked_in":
        return f"🔵 На смене (чек-ин: {a[5] or '—'})"
    if status == "checked_out":
        return f"⚪ Завершил (чек-аут: {a[7] or '—'})"
    if status == "cancelled":
        return "🚫 Отменён"
    return status


def _shift_duration_hours(shift: tuple) -> int:
    try:
        dt_start, dt_end = shift_start_end_local_naive(str(shift[2]), str(shift[3]), str(shift[4]))
        return max(1, int((dt_end - dt_start).total_seconds() // 3600))
    except Exception:
        return 1


def _parse_ts(value):
    if not value:
        return None
    s = str(value)
    s = s.replace(" ", "T", 1) if "T" not in s else s
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


@router.callback_query(F.data == "my_shifts")
async def show_my_shifts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    client = get_client(user_id)
    worker = get_worker(user_id)

    if client:
        shifts = list_shifts_for_client(user_id)
        if not shifts:
            await safe_edit_or_resend(callback, "📅 У вас пока нет смен.")
            await callback.answer()
            return

        text = "📅 Ваши смены:\n\n"
        keyboard_rows = []
        for s in shifts:
            status_emoji = "🟢" if s[6] == "open" else "🔵" if s[6] == "in_progress" else "⚪"
            d_ru = format_date_ru(s[1])
            text += f"{status_emoji} {d_ru} {s[2]}-{s[3]} | {s[5]}\n"
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{d_ru} {s[2]}-{s[3]}",
                        callback_data=f"shift_detail_{s[0]}",
                    )
                ]
            )
        keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
        await callback.answer()

    elif worker:
        shifts = list_shifts_for_worker(user_id)
        if not shifts:
            await safe_edit_or_resend(callback, "📅 У вас пока нет назначенных смен.")
            await callback.answer()
            return

        text = "📅 Ваши смены:\n\n"
        keyboard_rows = []
        for s in shifts:
            status_text = {
                "pending": "⏳ Ожидает",
                "confirmed": "✅ Подтверждена",
                "checked_in": "🔵 В процессе",
                "checked_out": "⚪ Завершена",
            }.get(s[5], s[5])
            d_ru = format_date_ru(s[1])
            text += f"{status_text}: {d_ru} {s[2]}-{s[3]}\n"
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{d_ru} {s[2]}-{s[3]}",
                        callback_data=f"worker_shift_{s[0]}",
                    )
                ]
            )
        keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
        await callback.answer()

    else:
        await safe_edit_or_resend(
            callback,
            "Не удалось определить роль. Нажмите /start и завершите регистрацию.",
        )
        await callback.answer()


@router.callback_query(F.data == "client_shift_statuses")
async def client_shift_statuses(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    shifts = list_shifts_for_client(user_id)
    if not shifts:
        await safe_edit_or_resend(callback, 
            "📡 Пока нет смен для контроля подтверждений.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "📡 Статус выхода на смену\n\nВыберите смену:\n"
    for s in shifts[:30]:
        sid, shift_date, st, et, _loc, project_name, _status = s
        text += f"• #{sid} {format_date_ru(shift_date)} {st}-{et} | {project_name}\n"
        rows.append([InlineKeyboardButton(text=f"Смена #{sid}", callback_data=f"shift_status_view_{sid}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    await safe_edit_or_resend(callback, 
        text,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data == "admin_shift_statuses")
async def admin_shift_statuses(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    shifts = list_open_shifts_admin(50)
    if not shifts:
        await safe_edit_or_resend(callback, 
            "📡 Нет открытых смен.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "📡 Статус выхода на смену (админ)\n\nВыберите смену:\n"
    for s in shifts:
        sid, shift_date, st, et, project_name, _status = s
        text += f"• #{sid} {format_date_ru(str(shift_date))} {st}-{et} | {project_name}\n"
        rows.append([InlineKeyboardButton(text=f"Смена #{sid}", callback_data=f"shift_status_view_{sid}")])
    rows.append([InlineKeyboardButton(text="🔙 В админ-панель", callback_data="admin_back")])
    await safe_edit_or_resend(callback, 
        text,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("shift_status_view_"))
async def shift_status_view(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("shift_status_view_", ""))
    user_id = callback.from_user.id
    is_admin = is_admin_user(user_id)
    if not is_admin and (not get_client(user_id) or not client_owns_shift(user_id, shift_id)):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    shift = get_shift(shift_id)
    if not shift:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return
    assignments = get_shift_assignments(shift_id)
    text = (
        f"📡 Статус подтверждения по смене #{shift_id}\n\n"
        f"📆 {format_date_ru(shift[2])} {shift[3]}-{shift[4]}\n"
        f"📍 {shift[5]}\n\n"
    )
    if not assignments:
        text += "Назначений пока нет.\n"
    else:
        pending_cnt = 0
        for a in assignments:
            name = _assign_worker_name(a)
            text += f"• {name}: {_assignment_status_line(a)}\n"
            if str(a[A_STATUS]) == "pending":
                pending_cnt += 1
        if pending_cnt == 0:
            text += "\n✅ Выход подтверждён всеми назначенными исполнителями.\n"
    rows = [
        [InlineKeyboardButton(text="🔔 Запросить подтверждение неподтвердивших", callback_data=f"shift_status_ping_{shift_id}")],
        [InlineKeyboardButton(text="🔁 Заменить исполнителя", callback_data=f"shift_replace_pick_{shift_id}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"shift_status_view_{shift_id}")],
    ]
    rows.append(
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_shift_statuses" if is_admin else "client_shift_statuses")]
    )
    await safe_edit_or_resend(callback, 
        text,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("shift_status_ping_"))
async def shift_status_ping(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("shift_status_ping_", ""))
    user_id = callback.from_user.id
    is_admin = is_admin_user(user_id)
    if not is_admin and (not get_client(user_id) or not client_owns_shift(user_id, shift_id)):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    pending = list_unconfirmed_assignments(shift_id)
    if not pending:
        await callback.answer("Все исполнители уже подтвердили выход.", show_alert=True)
        return
    sent = 0
    for _aid, worker_id, _st, _conf_at, full_name in pending:
        try:
            await callback.bot.send_message(
                int(worker_id),
                f"🔔 Напоминание по смене #{shift_id}: подтвердите выход в карточке смены.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Открыть смену", callback_data=f"worker_shift_{shift_id}")]
                    ]
                ),
            )
            sent += 1
        except Exception as e:
            logger.warning(
                "ping unconfirmed worker failed shift_id=%s worker_id=%s name=%s: %s",
                shift_id,
                worker_id,
                full_name,
                e,
                exc_info=True,
            )
    await callback.answer(f"Отправлено напоминаний: {sent}", show_alert=True)


@router.callback_query(F.data.startswith("shift_replace_pick_"))
async def shift_replace_pick(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("shift_replace_pick_", ""))
    user_id = callback.from_user.id
    is_admin = is_admin_user(user_id)
    if not is_admin and (not get_client(user_id) or not client_owns_shift(user_id, shift_id)):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    assignments = get_shift_assignments(shift_id)
    candidates = [a for a in assignments if str(a[A_STATUS]) in ("pending", "confirmed")]
    if not candidates:
        await callback.answer("Нет кандидатов для замены.", show_alert=True)
        return
    rows = []
    for a in candidates:
        name = _assign_worker_name(a)
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"Заменить: {name}",
                    callback_data=f"shift_replace_from_{shift_id}_{int(a[2])}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_status_view_{shift_id}")])
    await safe_edit_or_resend(callback, 
        "🔁 Выберите исполнителя, которого нужно заменить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("shift_replace_from_"))
async def shift_replace_from(callback: types.CallbackQuery):
    _, _, _, shift_raw, old_raw = callback.data.split("_", 4)
    shift_id = int(shift_raw)
    old_worker_id = int(old_raw)
    user_id = callback.from_user.id
    is_admin = is_admin_user(user_id)
    if not is_admin and (not get_client(user_id) or not client_owns_shift(user_id, shift_id)):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    assigned_ids = {int(a[2]) for a in get_shift_assignments(shift_id)}
    workers = get_workers_assignable()
    rows = []
    for w in workers:
        wid = int(w[0])
        if wid in assigned_ids:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{w[1]} ({w[3]})",
                    callback_data=f"shift_replace_to_{shift_id}_{old_worker_id}_{wid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_replace_pick_{shift_id}")])
    await safe_edit_or_resend(callback, 
        "👥 Выберите нового исполнителя на замену:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("shift_replace_to_"))
async def shift_replace_to(callback: types.CallbackQuery):
    _, _, _, shift_raw, old_raw, new_raw = callback.data.split("_", 5)
    shift_id = int(shift_raw)
    old_worker_id = int(old_raw)
    new_worker_id = int(new_raw)
    user_id = callback.from_user.id
    is_admin = is_admin_user(user_id)
    if not is_admin and (not get_client(user_id) or not client_owns_shift(user_id, shift_id)):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    result = replace_assignment_worker(shift_id, old_worker_id, new_worker_id)
    if not result.get("ok"):
        await callback.answer(f"Не удалось заменить: {result.get('reason')}", show_alert=True)
        return
    shift_owner = get_shift_with_owner(shift_id)
    try:
        await callback.bot.send_message(
            int(old_worker_id),
            f"ℹ️ Вы сняты со смены #{shift_id}. Если это ошибка — свяжитесь с менеджером.",
        )
    except Exception as e:
        logger.warning("notify old worker on replace shift_id=%s old=%s: %s", shift_id, old_worker_id, e, exc_info=True)
    try:
        await callback.bot.send_message(
            int(new_worker_id),
            f"📌 Вас назначили на смену #{shift_id}. Подтвердите выход в карточке смены.",
        )
    except Exception as e:
        logger.warning("notify new worker on replace shift_id=%s new=%s: %s", shift_id, new_worker_id, e, exc_info=True)
    try:
        await send_all_admins(
            callback.bot,
            f"🔁 Замена по смене #{shift_id}: {old_worker_id} → {new_worker_id}.",
        )
        if shift_owner and shift_owner[7]:
            await callback.bot.send_message(int(shift_owner[7]), f"🔁 По смене #{shift_id} выполнена замена исполнителя.")
    except Exception as e:
        logger.warning("notify admins/client on replace shift_id=%s: %s", shift_id, e, exc_info=True)
    try:
        log_shift_replacement(
            shift_id=shift_id,
            old_worker_id=old_worker_id,
            new_worker_id=new_worker_id,
            actor_user_id=callback.from_user.id,
            reason="manual_replace_from_status_screen",
        )
    except Exception as e:
        logger.warning("log_shift_replacement failed shift_id=%s: %s", shift_id, e, exc_info=True)
    shift = get_shift(shift_id)
    assignments = get_shift_assignments(shift_id)
    text = (
        f"📡 Статус подтверждения по смене #{shift_id}\n\n"
        f"📆 {format_date_ru(shift[2])} {shift[3]}-{shift[4]}\n"
        f"📍 {shift[5]}\n\n"
    )
    for a in assignments:
        name = _assign_worker_name(a)
        text += f"• {name}: {_assignment_status_line(a)}\n"
    rows = [
        [InlineKeyboardButton(text="🔔 Запросить подтверждение неподтвердивших", callback_data=f"shift_status_ping_{shift_id}")],
        [InlineKeyboardButton(text="🔁 Заменить исполнителя", callback_data=f"shift_replace_pick_{shift_id}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"shift_status_view_{shift_id}")],
        [
            InlineKeyboardButton(
                text="🔙 Назад",
                callback_data="admin_shift_statuses" if is_admin else "client_shift_statuses",
            )
        ],
    ]
    await safe_edit_or_resend(callback, 
        text,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer("Замена выполнена.", show_alert=True)


@router.callback_query(F.data == "my_projects")
async def my_projects(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    projects = list_projects_for_client(user_id)
    if not projects:
        await safe_edit_or_resend(callback, 
            "📋 Проектов пока нет. Администратор создаёт проект и привязывает его к вам.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    text = "📋 Ваши проекты:\n\nСмены — в «Мои смены». Здесь — всё по проекту: список смен и общий чат.\n\n"
    rows = []
    for p in projects:
        text += f"• #{p[0]} — {p[1]}\n"
        rows.append(
            [InlineKeyboardButton(text=f"📌 Экран проекта #{p[0]}", callback_data=f"project_hub_{p[0]}")]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
    await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data == "create_project")
async def create_project_client_stub(callback: types.CallbackQuery):
    await safe_edit_or_resend(callback, 
        "➕ Проект создаёт администратор (/admin → «Создать проект») и выбирает вас как заказчика.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
        ),
        parse_mode=None,
    )
    await callback.answer()


@router.callback_query(F.data == "client_shift_chats")
async def client_shift_chats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    shifts = list_shifts_for_client(user_id)
    if not shifts:
        await safe_edit_or_resend(callback, 
            "💬 У вас пока нет смен для чатов.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    for s in shifts[:25]:
        rows.append(
            [InlineKeyboardButton(text=f"💬 Смена #{s[0]}: {format_date_ru(s[1])} {s[2]}-{s[3]}", callback_data=f"chat_{s[0]}")]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    await safe_edit_or_resend(callback, 
        "💬 Выберите чат смены:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("shift_detail_"))
async def shift_detail(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("shift_detail_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа к этой смене.", show_alert=True)
        return

    shift = get_shift(shift_id)
    assignments = get_shift_assignments(shift_id)

    if not shift:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return

    text = (
        f"📅 Смена #{shift_id}\n\n"
        f"📆 Дата: {format_date_ru(shift[2])}\n"
        f"⏰ Время: {shift[3]} — {shift[4]}\n"
        f"📍 Локация: {shift[5]}\n"
        f"👥 Назначенные исполнители:\n"
    )

    for a in assignments:
        status_text = {
            "pending": "⏳ Ожидает",
            "confirmed": "✅ Подтвердил",
            "checked_in": "🔵 На смене",
            "checked_out": "⚪ Завершил",
        }.get(a[A_STATUS], a[A_STATUS])
        name = _assign_worker_name(a)
        checkin = str(a[5] or "—")
        checkout = str(a[7] or "—")
        text += f"• {name}: {status_text}\n  чек-ин: {checkin}\n  чек-аут: {checkout}\n"

    text += (
        "\n💡 Задание людям: кнопка «Поставить задачу исполнителям» — бот спросит название, описание и кого уведомить.\n"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Вся смена здесь", callback_data=f"shift_hub_cl_{shift_id}")],
            [InlineKeyboardButton(text="📝 Поставить задачу исполнителям", callback_data=f"add_task_{shift_id}")],
            [InlineKeyboardButton(text="✉️ Написать исполнителю", callback_data=f"msg_worker_pick_{shift_id}")],
            [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="📊 Отчёт", callback_data=f"report_{shift_id}")],
            [
                InlineKeyboardButton(text="📋 Проекты", callback_data="my_projects"),
                InlineKeyboardButton(text="✅ Все задачи", callback_data="my_client_tasks"),
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts"),
                InlineKeyboardButton(text="🏠 Меню", callback_data="main_menu"),
            ],
        ]
    )
    await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("worker_shift_"))
async def worker_shift_detail(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("worker_shift_", ""))
    user_id = callback.from_user.id
    auto_close_expired_breaks()
    shift = get_shift(shift_id)
    assignment = get_assignment(shift_id, user_id)

    if not shift or not assignment:
        await safe_edit_or_resend(callback, "❌ Смена не найдена или вы не назначены.")
        await callback.answer()
        return

    text = (
        f"📅 Смена #{shift_id}\n\n"
        f"📆 Дата: {format_date_ru(shift[2])}\n"
        f"⏰ Время: {shift[3]} — {shift[4]}\n"
        f"📍 Локация: {shift[5]}\n"
        f"💰 Ставка: {shift[6]} ₽/час\n\n"
        f"📊 Статус: "
    )

    status = assignment[3]
    status_text = {
        "pending": "⏳ Ожидает подтверждения",
        "confirmed": "✅ Вы подтвердили",
        "checked_in": "🔵 На смене",
        "checked_out": "⚪ Завершена",
    }.get(status, status)
    text += status_text

    keyboard_rows = []
    if status == "pending":
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить выход",
                    callback_data=f"confirm_shift_{shift_id}",
                )
            ]
        )
    elif status == "confirmed":
        keyboard_rows.append(
            [InlineKeyboardButton(text="✅ Чек-ин", callback_data=f"checkin_{shift_id}")]
        )
    elif status == "checked_in":
        keyboard_rows.append(
            [InlineKeyboardButton(text="✅ Чек-аут", callback_data=f"checkout_{shift_id}")]
        )
        active_break = get_active_break(shift_id, user_id)
        if active_break:
            keyboard_rows.append(
                [InlineKeyboardButton(text="▶️ Завершить перерыв", callback_data=f"break_stop_{shift_id}")]
            )
        else:
            keyboard_rows.append(
                [InlineKeyboardButton(text="⏸ Перерыв", callback_data=f"break_start_{shift_id}")]
            )

    keyboard_rows.append(
        [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")]
    )
    keyboard_rows.append(
        [InlineKeyboardButton(text="📋 Задачи смены", callback_data=f"tasks_{shift_id}")]
    )
    keyboard_rows.append(
        [InlineKeyboardButton(text="🎯 Вся смена здесь", callback_data=f"shift_hub_wk_{shift_id}")]
    )
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts"),
            InlineKeyboardButton(text="📋 Мои задачи", callback_data="my_tasks"),
        ]
    )
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts"),
            InlineKeyboardButton(text="🏠 Меню", callback_data="main_menu"),
        ]
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_shift_"))
async def confirm_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("confirm_shift_", ""))
    user_id = callback.from_user.id
    confirm_assignment(shift_id, user_id)
    shift_owner = get_shift_with_owner(shift_id)
    txt = f"✅ Исполнитель {callback.from_user.full_name} подтвердил выход на смену #{shift_id}."
    try:
        await send_all_admins(callback.bot, txt)
    except Exception as e:
        logger.warning("notify admins on confirm shift_id=%s: %s", shift_id, e, exc_info=True)
    try:
        if shift_owner and shift_owner[7]:
            await callback.bot.send_message(int(shift_owner[7]), txt)
    except Exception as e:
        logger.warning("notify client on confirm shift_id=%s: %s", shift_id, e, exc_info=True)
    await safe_edit_or_resend(callback, 
        f"✅ Вы подтвердили выход на смену #{shift_id}!",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 К смене", callback_data=f"worker_shift_{shift_id}")]
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("checkin_"))
async def checkin_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("checkin_", ""))
    await state.update_data(checkin_shift_id=shift_id)
    await state.set_state(CheckinFlow.geo)
    await safe_edit_or_resend(callback, 
        "📍 ЧЕК-ИН\n\n"
        "Сделайте по шагам:\n"
        "1) Отправьте локацию (скрепка 📎 -> Локация)\n"
        "2) Дождитесь сообщения от бота\n"
        "3) Отправьте селфи фото (не как файл)\n\n"
        "⚠️ Одним сообщением чек-ин отправить нельзя.",
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("break_start_"))
async def break_start_menu(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("break_start_", ""))
    auto_close_expired_breaks()
    assignment = get_assignment(shift_id, callback.from_user.id)
    if not assignment or assignment[3] != "checked_in":
        await callback.answer("Перерыв доступен только во время смены (после чек-ина).", show_alert=True)
        return
    if get_active_break(shift_id, callback.from_user.id):
        await callback.answer("У вас уже активен перерыв.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🍽 Обед до 30 мин", callback_data=f"break_type_{shift_id}_lunch")],
            [InlineKeyboardButton(text="🚬 Перекур до 5 мин", callback_data=f"break_type_{shift_id}_smoke")],
            [InlineKeyboardButton(text="🚻 Тех. перерыв до 10 мин", callback_data=f"break_type_{shift_id}_tech")],
            [InlineKeyboardButton(text="🔙 Назад к смене", callback_data=f"worker_shift_{shift_id}")],
        ]
    )
    await safe_edit_or_resend(callback, "⏸ Выберите тип перерыва:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("break_type_"))
async def break_type_start(callback: types.CallbackQuery):
    _, _, shift_raw, break_type = callback.data.split("_", 3)
    shift_id = int(shift_raw)
    auto_close_expired_breaks()
    assignment = get_assignment(shift_id, callback.from_user.id)
    if not assignment or assignment[3] != "checked_in":
        await callback.answer("Недоступно: нет активной смены.", show_alert=True)
        return
    shift = get_shift(shift_id)
    stats = get_worker_break_stats(shift_id, callback.from_user.id)
    now = now_local_naive()
    checkin_time = _parse_ts(assignment[5])
    if break_type == "lunch" and stats["lunch_count"] >= 1:
        await callback.answer("Обеденный перерыв можно взять только один раз за смену.", show_alert=True)
        return
    if break_type == "lunch":
        if not checkin_time or (now - checkin_time).total_seconds() < 2 * 3600:
            await callback.answer("Обед доступен не раньше чем через 2 часа после чек-ина.", show_alert=True)
            return
        try:
            _dt_start, dt_end = shift_start_end_local_naive(str(shift[2]), str(shift[3]), str(shift[4]))
            if (dt_end - now).total_seconds() < 2 * 3600:
                await callback.answer("Обед недоступен в последние 2 часа смены.", show_alert=True)
                return
        except Exception:
            pass
    if break_type == "smoke":
        if not checkin_time or (now - checkin_time).total_seconds() < 3600:
            await callback.answer("Перекур доступен не раньше чем через 1 час после чек-ина.", show_alert=True)
            return
        try:
            _dt_start, dt_end = shift_start_end_local_naive(str(shift[2]), str(shift[3]), str(shift[4]))
            if (dt_end - now).total_seconds() < 3600:
                await callback.answer("Перекур недоступен в последний час смены.", show_alert=True)
                return
        except Exception:
            pass
        max_smokes = max(0, _shift_duration_hours(shift) - 1)
        if stats["smoke_count"] >= max_smokes:
            await callback.answer(
                f"Лимит перекуров на эту смену исчерпан: {max_smokes}.",
                show_alert=True,
            )
            return
    if break_type == "tech":
        if not checkin_time or (now - checkin_time).total_seconds() < 15 * 60:
            await callback.answer("Тех. перерыв доступен не раньше чем через 15 минут после чек-ина.", show_alert=True)
            return
    ok = start_assignment_break(shift_id, callback.from_user.id, break_type, "")
    if not ok:
        await callback.answer("Перерыв уже запущен.", show_alert=True)
        return
    label = {
        "lunch": "Обед до 30 минут. Если не завершите сами, бот закроет его автоматически.",
        "smoke": "Перекур до 5 минут. Если не завершите сами, бот закроет его автоматически.",
        "tech": "Тех. перерыв до 10 минут. Если не завершите сами, бот закроет его автоматически.",
    }.get(break_type, f"Перерыв начат ({break_type}).")
    await safe_edit_or_resend(callback, 
        f"⏸ {label}\n\nНажмите «Завершить перерыв», когда вернётесь.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="▶️ Завершить перерыв", callback_data=f"break_stop_{shift_id}")],
                [InlineKeyboardButton(text="🔙 К смене", callback_data=f"worker_shift_{shift_id}")],
            ]
        ),
    )
    await callback.answer("Перерыв запущен.")


@router.callback_query(F.data.startswith("break_stop_"))
async def break_stop_now(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("break_stop_", ""))
    auto_close_expired_breaks()
    ok = stop_assignment_break(shift_id, callback.from_user.id)
    if not ok:
        await callback.answer("Активный перерыв не найден.", show_alert=True)
        return
    await safe_edit_or_resend(callback, 
        "▶️ Перерыв завершён.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 К смене", callback_data=f"worker_shift_{shift_id}")]]
        ),
    )
    await callback.answer("Перерыв завершён.")


@router.message(F.location, CheckinFlow.geo)
async def checkin_geo_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("checkin_shift_id")
    shift = get_shift(int(shift_id)) if shift_id else None
    assignment = get_assignment(int(shift_id), message.from_user.id) if shift_id else None
    if not shift or not assignment or assignment[3] != "confirmed":
        await message.answer("❌ Для чек-ина смена должна быть подтверждена.")
        await state.clear()
        return
    dt_start = _shift_start(shift)
    now = now_local_naive()
    if now < dt_start.replace(second=0, microsecond=0) and (dt_start - now).total_seconds() > 30 * 60:
        await message.answer(
            "⏳ Слишком рано для чек-ина. Чек-ин доступен за 30 минут до старта смены."
        )
        return
    exp_lat, exp_lng, radius = _shift_geo_limits(shift)
    if exp_lat is not None and exp_lng is not None:
        dist = _distance_m(
            float(message.location.latitude),
            float(message.location.longitude),
            float(exp_lat),
            float(exp_lng),
        )
        if dist > radius:
            set_assignment_checkin_geo_failed(int(shift_id), message.from_user.id)
            await message.answer(
                "❌ Вы слишком далеко от площадки для чек-ина.\n"
                f"Расстояние: {int(dist)} м, допустимый радиус: {radius} м.\n"
                "Подойдите к площадке и отправьте геолокацию снова."
            )
            return
    await state.update_data(
        checkin_lat=message.location.latitude,
        checkin_lng=message.location.longitude,
    )
    await state.set_state(CheckinFlow.photo)
    await message.answer(
        "📸 ЧЕК-ИН\n\nОтправьте селфи на фоне объекта.\n\nСкрепка 📎 → Камера",
        parse_mode=None,
    )


@router.message(CheckinFlow.geo)
async def checkin_geo_wrong_payload(message: types.Message):
    await message.answer(
        "⚠️ Для этого шага нужна именно геолокация.\n"
        "Нажмите скрепку 📎 -> Локация и отправьте текущую точку.",
        parse_mode=None,
    )


@router.message(F.photo, CheckinFlow.photo)
async def checkin_photo_received(message: types.Message, state: FSMContext):
    try:
        photo_id = message.photo[-1].file_id
        data = await state.get_data()
        shift_id = data["checkin_shift_id"]
        shift_row = get_shift(int(shift_id))
        exp_lat, exp_lng, _ = _shift_geo_limits(shift_row)
        lat = data.get("checkin_lat")
        lng = data.get("checkin_lng")
        if exp_lat is not None and exp_lng is not None:
            ok = do_checkin(
                int(shift_id),
                message.from_user.id,
                photo_id,
                float(lat) if lat is not None else None,
                float(lng) if lng is not None else None,
                1,
            )
        else:
            ok = do_checkin(
                int(shift_id),
                message.from_user.id,
                photo_id,
                float(lat) if lat is not None else None,
                float(lng) if lng is not None else None,
                None,
            )
        if not ok:
            await message.answer(
                "❌ Чек-ин не сохранился: назначение не найдено или смена уже в другом состоянии. "
                "Откройте смену из «Мои смены» и попробуйте снова."
            )
            await state.clear()
            return
        shift_owner = get_shift_with_owner(int(shift_id))
        if shift_owner:
            dt_start = _shift_start(shift_owner)
            if now_local_naive() > dt_start:
                mark_assignment_event_by_shift_worker(int(shift_id), message.from_user.id, "late_checkin_notified_at")
                delay_min = int((now_local_naive() - dt_start).total_seconds() // 60)
                late_text = (
                    f"⚠️ Исполнитель {message.from_user.full_name} отметил чек-ин по смене #{shift_id} "
                    f"с опозданием на {max(delay_min, 1)} мин."
                )
                client_text = (
                    f"⚠️ По смене #{shift_id} чек-ин исполнителя выполнен с опозданием примерно на {max(delay_min, 1)} мин."
                )
                try:
                    await send_all_admins(message.bot, late_text)
                    client_id = shift_owner[7]
                    if client_id:
                        await message.bot.send_message(int(client_id), client_text)
                except Exception as e:
                    logger.warning("late checkin notify shift_id=%s: %s", shift_id, e, exc_info=True)
        await message.answer(
            "✅ " + bold("Чек-ин выполнен!") + " " + em("Удачной смены! 🚀"),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        await state.clear()
    except Exception:
        logger.exception(
            "checkin_photo_received failed user_id=%s state=%s",
            message.from_user.id,
            await state.get_data(),
        )
        await message.answer("❌ Не удалось завершить чек-ин из-за внутренней ошибки. Попробуйте ещё раз.")
        await state.clear()


@router.message(CheckinFlow.photo)
async def checkin_photo_wrong_payload(message: types.Message):
    await message.answer(
        "⚠️ Сейчас нужно отправить селфи фото.\n"
        "Отправьте изображение как фото (не как файл/document).",
        parse_mode=None,
    )


@router.callback_query(F.data.startswith("checkout_"))
async def checkout_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("checkout_", ""))
    assignment = get_assignment(shift_id, callback.from_user.id)
    if not assignment or assignment[3] != "checked_in":
        await callback.answer("Чек-аут доступен после чек-ина на смене.", show_alert=True)
        return
    await state.update_data(checkout_shift_id=shift_id)
    await state.set_state(CheckoutFlow.geo)
    await safe_edit_or_resend(callback, 
        "📍 ЧЕК-АУТ\n\n"
        "По шагам:\n"
        "1) Отправьте геолокацию (скрепка 📎 → Локация)\n"
        "2) Затем селфи фото (не как файл)\n\n"
        "⚠️ Одним сообщением чек-аут отправить нельзя.",
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
    await callback.answer()


@router.message(F.location, CheckoutFlow.geo)
async def checkout_geo_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("checkout_shift_id")
    shift = get_shift(int(shift_id)) if shift_id else None
    assignment = get_assignment(int(shift_id), message.from_user.id) if shift_id else None
    if not shift or not assignment or assignment[3] != "checked_in":
        await message.answer("❌ Чек-аут недоступен. Откройте смену из «Мои смены».")
        await state.clear()
        return
    exp_lat, exp_lng, radius = _shift_geo_limits(shift)
    if exp_lat is not None and exp_lng is not None:
        dist = _distance_m(
            float(message.location.latitude),
            float(message.location.longitude),
            float(exp_lat),
            float(exp_lng),
        )
        if dist > radius:
            await message.answer(
                "❌ Вы слишком далеко от площадки для чек-аута.\n"
                f"Расстояние: {int(dist)} м, допустимый радиус: {radius} м.\n"
                "Подойдите к площадке и отправьте геолокацию снова."
            )
            return
    await state.update_data(
        checkout_lat=message.location.latitude,
        checkout_lng=message.location.longitude,
    )
    await state.set_state(CheckoutFlow.photo)
    await message.answer(
        "📸 ЧЕК-АУТ\n\n"
        "Отправьте селфи на фоне объекта (как при чек-ине).\n\n"
        "Скрепка 📎 → Камера",
        parse_mode=None,
    )


@router.message(CheckoutFlow.geo)
async def checkout_geo_wrong_payload(message: types.Message):
    await message.answer(
        "⚠️ Нужна геолокация для чек-аута.\nСкрепка 📎 → Локация.",
        parse_mode=None,
    )


@router.message(F.photo, CheckoutFlow.photo)
async def checkout_photo_received(message: types.Message, state: FSMContext):
    try:
        photo_id = message.photo[-1].file_id
        data = await state.get_data()
        shift_id = data["checkout_shift_id"]
        lat = data.get("checkout_lat")
        lng = data.get("checkout_lng")
        # Защита от "залипшего" состояния: без шага гео чек-аут не выполняем.
        if lat is None or lng is None:
            await state.set_state(CheckoutFlow.geo)
            await message.answer(
                "⚠️ Для чек-аута сначала отправьте геолокацию (скрепка 📎 -> Локация), затем фото.",
                parse_mode=None,
            )
            return
        ok = do_checkout(int(shift_id), message.from_user.id, photo_id)
        if not ok:
            await message.answer(
                "❌ Чек-аут не сохранился. Убедитесь, что вы на смене (статус «На смене») и открыли чек-аут из карточки."
            )
            await state.clear()
            return
        await message.answer(
            "✅ " + bold("Смена завершена!") + " " + em("Спасибо за работу!"),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        shift_after = get_shift(int(shift_id))
        if shift_after and str(shift_after[7] or "") == "closed":
            await _notify_shift_closed_summary(message.bot, int(shift_id), "чек-аут исполнителя")
        else:
            await _notify_admins_checkout_partial(
                message.bot,
                int(shift_id),
                message.from_user.full_name or str(message.from_user.id),
            )
        await state.clear()
    except Exception:
        logger.exception(
            "checkout_photo_received failed user_id=%s state=%s",
            message.from_user.id,
            await state.get_data(),
        )
        await message.answer("❌ Не удалось завершить чек-аут из-за внутренней ошибки. Попробуйте ещё раз.")
        await state.clear()


@router.message(F.text, CheckoutFlow.photo)
async def checkout_skip_photo(message: types.Message, state: FSMContext):
    await message.answer(
        "Для чек-аута нужно фото (селфи). Отправьте снимок — так же, как при чек-ине."
    )


@router.message(CheckoutFlow.photo, ~F.photo)
async def checkout_photo_wrong_type(message: types.Message):
    await message.answer(
        "⚠️ Отправьте изображение именно как фото (камера), не как файл.",
        parse_mode=None,
    )


@router.callback_query(F.data.startswith("forgot_close_"))
async def forgot_close_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("forgot_close_", ""))
    ok = do_checkout(shift_id, callback.from_user.id, None)
    if not ok:
        await callback.answer("Не удалось закрыть смену (нет чек-ина или уже закрыта).", show_alert=True)
        return
    shift_after = get_shift(int(shift_id))
    if shift_after and str(shift_after[7] or "") == "closed":
        await _notify_shift_closed_summary(callback.bot, shift_id, "принудительное закрытие исполнителем")
    else:
        await _notify_admins_checkout_partial(
            callback.bot,
            shift_id,
            callback.from_user.full_name or str(callback.from_user.id),
        )
    await safe_edit_or_resend(callback, "✅ Смена закрыта. Спасибо!")
    await callback.answer()


@router.callback_query(F.data.startswith("forgot_extend_"))
async def forgot_extend_shift(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("forgot_extend_", ""))
    await state.update_data(extend_shift_id=shift_id)
    await state.set_state(ShiftExtensionFlow.minutes)
    await callback.message.answer(
        em("На сколько минут продлить смену? Введите число, например ") + bold("60") + em("."),
        parse_mode=PARSE_MODE_TELEGRAM,
    )
    await callback.answer()


@router.message(ShiftExtensionFlow.minutes)
async def forgot_extend_minutes(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer(
            em("Введите число минут, например ") + bold("60") + em("."),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        return
    minutes = int(raw)
    if minutes < 15 or minutes > 240:
        await message.answer("Допустимый диапазон продления: 15..240 минут.")
        return
    data = await state.get_data()
    shift_id = int(data.get("extend_shift_id"))
    ok = set_extension_request(shift_id, message.from_user.id, minutes)
    if not ok:
        await message.answer("❌ Не удалось отправить запрос на продление.")
        await state.clear()
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить продление", callback_data=f"admin_ext_ok_{shift_id}_{message.from_user.id}_{minutes}")],
            [InlineKeyboardButton(text="❌ Отказать", callback_data=f"admin_ext_no_{shift_id}_{message.from_user.id}_{minutes}")],
        ]
    )
    await send_all_admins(
        message.bot,
        f"⏱ Запрос продления смены #{shift_id} на {minutes} минут от исполнителя {message.from_user.full_name}.",
        reply_markup=kb,
    )
    shift_owner = get_shift_with_owner(shift_id)
    if shift_owner and shift_owner[7]:
        await message.bot.send_message(
            int(shift_owner[7]),
            f"ℹ️ Исполнитель запросил продление смены #{shift_id} на {minutes} минут. Ожидается решение администратора.",
        )
    await message.answer("✅ Запрос отправлен администратору. Ждём решение.")
    await state.clear()


@router.callback_query(F.data.startswith("admin_ext_ok_"))
async def admin_extension_approve(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, _, _, shift_raw, worker_raw, min_raw = callback.data.split("_", 5)
    shift_id = int(shift_raw)
    worker_id = int(worker_raw)
    minutes = int(min_raw)
    resolve_extension_request(shift_id, worker_id, approved=True)
    extend_shift_end_time(shift_id, minutes)
    await callback.bot.send_message(worker_id, f"✅ Продление смены #{shift_id} на {minutes} минут одобрено администратором.")
    await safe_edit_or_resend(callback, f"✅ Продление по смене #{shift_id} одобрено.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_ext_no_"))
async def admin_extension_reject(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, _, _, shift_raw, worker_raw, min_raw = callback.data.split("_", 5)
    shift_id = int(shift_raw)
    worker_id = int(worker_raw)
    minutes = int(min_raw)
    resolve_extension_request(shift_id, worker_id, approved=False)
    closed = do_checkout(shift_id, worker_id, None)
    if closed:
        await _notify_shift_closed_summary(callback.bot, shift_id, "отклонено продление администратором")
        await callback.bot.send_message(
            worker_id,
            f"❌ Продление на {minutes} минут отклонено. Смена #{shift_id} закрыта.",
        )
        await safe_edit_or_resend(callback, f"✅ Продление отклонено, смена #{shift_id} закрыта.")
    else:
        await callback.bot.send_message(
            worker_id,
            f"❌ Продление на {minutes} минут отклонено. Закройте смену #{shift_id} вручную (чек-аут в карточке).",
        )
        await safe_edit_or_resend(callback, 
            f"Продление отклонено. Автозакрытие не удалось — проверьте статус смены #{shift_id}."
        )
    await callback.answer()


@router.callback_query(F.data.startswith("msg_worker_pick_"))
async def client_pick_worker_to_message(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("msg_worker_pick_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    assignments = get_shift_assignments(shift_id)
    if not assignments:
        await safe_edit_or_resend(callback, 
            "❌ На смену пока никто не назначен.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    for a in assignments:
        worker_id = int(a[2])
        worker_name = _assign_worker_name(a) or f"id {worker_id}"
        rows.append([InlineKeyboardButton(text=f"{worker_name}", callback_data=f"msg_worker_to_{shift_id}_{worker_id}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")])
    await safe_edit_or_resend(callback, 
        "✉️ Выберите исполнителя, которому отправить сообщение:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("msg_worker_to_"))
async def client_message_worker_start(callback: types.CallbackQuery, state: FSMContext):
    raw = callback.data.replace("msg_worker_to_", "")
    parts = raw.split("_")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        await callback.answer()
        return
    shift_id = int(parts[0])
    worker_id = int(parts[1])
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(client_msg_shift_id=shift_id, client_msg_worker_id=worker_id)
    await state.set_state(ClientMessageWorkerFlow.text)
    await callback.message.answer("Введите текст сообщения исполнителю:")
    await callback.answer()


@router.message(ClientMessageWorkerFlow.text)
async def client_message_worker_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data.get("client_msg_shift_id"))
    worker_id = int(data.get("client_msg_worker_id"))
    client_row = get_client(message.from_user.id)
    if not client_row or not client_owns_shift(message.from_user.id, shift_id):
        await message.answer("❌ Сессия недействительна.")
        await state.clear()
        return
    sender = client_row[2] or "Заказчик"
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Пустое сообщение нельзя отправить.")
        return
    try:
        body = (
            bold("Сообщение от заказчика")
            + "\n\n"
            + em(f"Смена #{shift_id}")
            + "\n"
            + em(f"От: {sender}")
            + "\n\n"
            + em(text)
        )
        await message.bot.send_message(worker_id, body, parse_mode=PARSE_MODE_TELEGRAM)
        await message.answer("✅ Сообщение отправлено исполнителю.")
    except Exception as e:
        logger.warning(
            "client_message_worker_send failed worker_id=%s shift_id=%s: %s",
            worker_id,
            shift_id,
            e,
            exc_info=True,
        )
        await message.answer("❌ Не удалось отправить сообщение исполнителю.")
    await state.clear()


def _report_tabs_keyboard(shift_id: int, tab: str, task_filter: str = "all") -> InlineKeyboardMarkup:
    def mark(tab_id: str, label: str) -> str:
        return f"• {label}" if tab == tab_id else label

    rows = [
        [
            InlineKeyboardButton(text=mark("people", "Исполнители"), callback_data=f"report_tab_{shift_id}_people"),
            InlineKeyboardButton(text=mark("tasks", "Задачи"), callback_data=f"report_tab_{shift_id}_tasks"),
            InlineKeyboardButton(text=mark("breaks", "Перерывы"), callback_data=f"report_tab_{shift_id}_breaks"),
        ],
    ]
    if tab == "tasks":
        def fmark(v: str, label: str) -> str:
            return f"• {label}" if task_filter == v else label
        rows.append(
            [
                InlineKeyboardButton(text=fmark("all", "все"), callback_data=f"report_task_filter_{shift_id}_all"),
                InlineKeyboardButton(text=fmark("open", "невыполненные"), callback_data=f"report_task_filter_{shift_id}_open"),
                InlineKeyboardButton(text=fmark("overdue", "просроченные"), callback_data=f"report_task_filter_{shift_id}_overdue"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="✉️ Пингнуть всех по просроченным задачам",
                    callback_data=f"report_ping_overdue_{shift_id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="📤 Поделиться сводкой", callback_data=f"report_share_{shift_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _render_report_text(
    shift_id: int,
    report: dict,
    tab: str,
    task_filter: str = "all",
    *,
    include_financials: bool = False,
) -> str:
    shift = report["shift"]
    tasks = report.get("tasks") or []
    assignments = report.get("assignments") or []
    breaks = get_shift_breaks(shift_id)

    # SLA-индикаторы
    pending = sum(1 for a in assignments if str(a[A_STATUS]) == "pending")
    late = sum(1 for a in assignments if str(a[17] or "") or str(a[18] or ""))
    if pending > 0 or late > 0:
        sla = "🔴 риск"
    elif all(str(a[A_STATUS]) in ("confirmed", "checked_in", "checked_out") for a in assignments):
        sla = "🟢 ок"
    else:
        sla = "🟡 внимание"

    rate_line = f"💰 Ставка: {shift[6]} ₽/час\n" if include_financials else ""
    header = (
        f"📊 ОТЧЁТ ПО СМЕНЕ #{shift_id}\n\n"
        f"📆 {format_date_ru(shift[2])} | {shift[3]}-{shift[4]}\n"
        f"📍 {shift[5]}\n"
        f"{rate_line}"
        f"SLA: {sla}\n\n"
    )

    if tab == "people":
        text = header + "👥 ИСПОЛНИТЕЛИ\n"
        total_payment = 0.0
        tech_totals: dict[int, int] = {}
        for b in breaks:
            if str(b[3] or "") != "tech":
                continue
            st_dt = _parse_ts(b[4])
            en_dt = _parse_ts(b[5])
            if st_dt and en_dt and en_dt >= st_dt:
                wid = int(b[2])
                tech_totals[wid] = tech_totals.get(wid, 0) + int((en_dt - st_dt).total_seconds() // 60)
        for a in assignments:
            hours = float(a[9] or 0)
            billed_h = float(a[26] or hours) if len(a) > 26 else hours
            penalty_h = float(a[27] or 0) if len(a) > 27 else 0
            payment = float(a[10] or 0)
            if include_financials:
                total_payment += payment
            status = "✅" if a[A_STATUS] == "checked_out" else "⏳"
            name = _assign_worker_name(a)
            extra = f" (штраф {penalty_h:.1f} ч)" if penalty_h > 0 else ""
            tech_extra = ""
            tech_total = tech_totals.get(int(a[2]), 0)
            if tech_total > 30:
                tech_extra = f", ⚠️ тех. перерывы {tech_total} мин"
            pay_seg = f", {payment:.0f} ₽" if include_financials else ""
            text += (
                f"{status} {name}: факт {hours:.1f} ч, "
                f"{'к оплате' if include_financials else 'к зачёту'} {billed_h:.1f} ч{extra}{pay_seg}{tech_extra}\n"
            )
        if include_financials:
            text += f"\n💰 ИТОГО: {total_payment:.0f} ₽"
        return text

    if tab == "tasks":
        text = header + "📋 ЗАДАЧИ\n"
        if not tasks:
            return text + "• задач нет\n"
        done = 0
        shown = 0
        shift_end = _parse_ts(f"{shift[2]} {str(shift[4])[:5]}") if shift and shift[2] and shift[4] else None
        if shift_end and shift and str(shift[4])[:5] <= str(shift[3])[:5]:
            # Ночная смена (через полночь)
            shift_end = shift_end + timedelta(days=1)
        for t in tasks:
            title = t[2] or "Задача"
            t_status = str(t[5] or "")
            assigned_at = _parse_ts(t[-2] if len(t) >= 13 else None)
            completed_at = _parse_ts(t[6] if len(t) > 6 else None)
            worker_name = str(t[-1] or "Не назначен")
            is_open = t_status != "completed"
            is_overdue = bool(is_open and shift_end and now_local_naive() > shift_end)
            if task_filter == "open" and not is_open:
                continue
            if task_filter == "overdue" and not is_overdue:
                continue
            shown += 1
            icon = "✅" if t_status == "completed" else "⏳"
            if t_status == "completed":
                done += 1
            dur_line = ""
            if assigned_at and completed_at and completed_at >= assigned_at:
                mins = int((completed_at - assigned_at).total_seconds() // 60)
                dur_line = f", время: {mins} мин"
            elif completed_at:
                dur_line = f", закрыта: {completed_at.strftime('%d.%m %H:%M')}"
            if is_overdue:
                dur_line += ", ⚠️ просрочена"
            text += f"{icon} {title} — {worker_name}{dur_line}\n"
        if shown == 0:
            text += "• по выбранному фильтру задач нет\n"
        text += f"\nИтого задач: {len(tasks)}, выполнено: {done}, показано: {shown}"
        return text

    text = header + "⏸ ПЕРЕРЫВЫ\n"
    if not breaks:
        return text + "• перерывы не фиксировались\n"
    by_worker: dict[int, int] = {}
    by_name: dict[int, str] = {}
    for b in breaks:
        worker_id = int(b[2])
        by_name[worker_id] = str(b[7] or worker_id)
        st_dt = _parse_ts(b[4])
        en_dt = _parse_ts(b[5])
        if st_dt and en_dt and en_dt >= st_dt:
            by_worker[worker_id] = by_worker.get(worker_id, 0) + int((en_dt - st_dt).total_seconds() // 60)
    for wid, name in by_name.items():
        total_m = by_worker.get(wid, 0)
        warn = " ⚠️ риск" if total_m > 30 else ""
        text += f"• {name}: суммарно {total_m} мин{warn}\n"
    text += "\nПерерывы фиксируются для контроля; в расчёт оплаты пока не вычитаются."
    return text


async def _notify_admins_checkout_partial(bot: Bot, shift_id: int, worker_label: str) -> None:
    """Уведомление админов о чек-ауте одного исполнителя, пока смена целиком не закрыта."""
    shift_row = get_shift(int(shift_id))
    if not shift_row or str(shift_row[7] or "") == "closed":
        return
    try:
        await send_all_admins(
            bot,
            f"✅ Чек-аут по смене #{shift_id}\n"
            f"Исполнитель: {worker_label}\n\n"
            "Смена в системе ещё не закрыта полностью (есть другие назначения). "
            "Итоговый отчёт уйдёт администраторам и заказчику, когда все исполнители завершат смену.",
            parse_mode=None,
        )
    except Exception as e:
        logger.warning(
            "notify admins partial checkout shift_id=%s: %s",
            shift_id,
            e,
            exc_info=True,
        )


async def _notify_shift_closed_summary(bot: Bot, shift_id: int, reason: str) -> None:
    """Отправляет автосводку при полном закрытии смены."""
    shift = get_shift(shift_id)
    if not shift or str(shift[7] or "") != "closed":
        return
    report = get_shift_report(shift_id)
    if not report.get("shift"):
        return
    summary_client = _render_report_text(shift_id, report, "people", include_financials=False)
    summary_admin = _render_report_text(shift_id, report, "people", include_financials=True)
    body_client = f"✅ Смена #{shift_id} закрыта ({reason}).\n\n{summary_client}"
    body_admin = f"✅ Смена #{shift_id} закрыта ({reason}).\n\n{summary_admin}"
    try:
        shift_owner = get_shift_with_owner(shift_id)
        client_id = int(shift_owner[7]) if shift_owner and shift_owner[7] else None
        if client_id:
            await bot.send_message(client_id, body_client, parse_mode=None)
    except Exception as e:
        logger.warning("auto summary to client failed shift_id=%s: %s", shift_id, e, exc_info=True)
    try:
        await send_all_admins(bot, body_admin, parse_mode=None)
    except Exception as e:
        logger.warning("auto summary to admins failed shift_id=%s: %s", shift_id, e, exc_info=True)


@router.callback_query(F.data.startswith("report_share_"))
async def report_share(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("report_share_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = get_shift_report(shift_id)
    if not report["shift"]:
        await callback.answer("Смена не найдена.", show_alert=True)
        return
    text = _render_report_text(shift_id, report, "people")
    await callback.message.answer("📤 Краткая сводка для пересылки:\n\n" + text, parse_mode=None)
    await callback.answer("Сводка отправлена ниже.")


@router.callback_query(F.data.startswith("report_tab_"))
async def show_shift_report_tab(callback: types.CallbackQuery):
    raw = callback.data.replace("report_tab_", "")
    parts = raw.split("_", 1)
    if len(parts) != 2 or not parts[0].isdigit():
        await callback.answer()
        return
    shift_id = int(parts[0])
    tab = parts[1] if parts[1] in ("people", "tasks", "breaks") else "people"
    task_filter = "all"
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = get_shift_report(shift_id)
    if not report["shift"]:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return
    text = _render_report_text(shift_id, report, tab, task_filter)
    await safe_edit_or_resend(callback, 
        text,
        reply_markup=_report_tabs_keyboard(shift_id, tab, task_filter),
        parse_mode=None,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("report_task_filter_"))
async def report_task_filter(callback: types.CallbackQuery):
    raw = callback.data.replace("report_task_filter_", "")
    parts = raw.split("_", 1)
    if len(parts) != 2 or not parts[0].isdigit():
        await callback.answer()
        return
    shift_id = int(parts[0])
    task_filter = parts[1] if parts[1] in ("all", "open", "overdue") else "all"
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = get_shift_report(shift_id)
    if not report["shift"]:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return
    text = _render_report_text(shift_id, report, "tasks", task_filter)
    await safe_edit_or_resend(callback, 
        text,
        reply_markup=_report_tabs_keyboard(shift_id, "tasks", task_filter),
        parse_mode=None,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("report_ping_overdue_"))
async def report_ping_overdue(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("report_ping_overdue_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = get_shift_report(shift_id)
    shift = report.get("shift")
    if not shift:
        await callback.answer("Смена не найдена.", show_alert=True)
        return
    shift_end = _parse_ts(f"{shift[2]} {str(shift[4])[:5]}")
    if shift_end and str(shift[4])[:5] <= str(shift[3])[:5]:
        shift_end = shift_end + timedelta(days=1)
    if not shift_end or now_local_naive() <= shift_end:
        await callback.answer("Смена ещё не завершена — просрочек по SLA пока нет.", show_alert=True)
        return

    tasks = report.get("tasks") or []
    per_worker: dict[int, list[str]] = {}
    for t in tasks:
        status = str(t[5] or "")
        if status == "completed":
            continue
        wid = t[4] if len(t) > 4 else None
        if wid is None:
            continue
        worker_id = int(wid)
        title = str(t[2] or "Задача")
        per_worker.setdefault(worker_id, []).append(title)

    if not per_worker:
        await callback.answer("Нет открытых просроченных задач для пинга.", show_alert=True)
        return

    sent = 0
    for wid, titles in per_worker.items():
        preview = "\n".join([f"• {x}" for x in titles[:8]])
        if len(titles) > 8:
            preview += f"\n• … и ещё {len(titles) - 8}"
        try:
            await callback.bot.send_message(
                int(wid),
                "⚠️ Напоминание по просроченным задачам\n\n"
                f"Смена #{shift_id} уже завершилась, но у вас есть невыполненные задачи:\n"
                f"{preview}\n\n"
                "Пожалуйста, завершите задачи или свяжитесь с координатором.",
                parse_mode=None,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Открыть задачи смены", callback_data=f"tasks_{shift_id}")]
                    ]
                ),
            )
            sent += 1
            record_overdue_task_ping(shift_id, int(wid))
        except Exception as e:
            logger.warning(
                "report_ping_overdue send failed shift_id=%s worker_id=%s: %s",
                shift_id,
                wid,
                e,
                exc_info=True,
            )
    await callback.answer(f"Пинг отправлен {sent} исполнителям.", show_alert=True)


@router.callback_query(F.data.startswith("report_"))
async def show_shift_report(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("report_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    report = get_shift_report(shift_id)
    if not report["shift"]:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return
    text = _render_report_text(shift_id, report, "people")
    await safe_edit_or_resend(callback, 
        text,
        reply_markup=_report_tabs_keyboard(shift_id, "people", "all"),
        parse_mode=None,
    )
    await callback.answer()
