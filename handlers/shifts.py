# handlers/shifts.py
import math
from datetime import datetime

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import (
    get_client,
    get_worker,
    get_shift,
    get_shift_assignments,
    get_assignment,
    confirm_assignment,
    do_checkin,
    do_checkout,
    get_shift_report,
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
)
from config import ADMIN_USER_ID
from states import CheckinFlow, CheckoutFlow, ShiftExtensionFlow, ClientMessageWorkerFlow

router = Router()

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
    lat = shift[9]
    lng = shift[10]
    radius = int(shift[11] or 300)
    return lat, lng, radius


def _shift_start(shift: tuple) -> datetime:
    raw = str(shift[3] or "").strip()
    hhmm = raw[:5] if len(raw) >= 5 else raw
    return datetime.strptime(f"{shift[2]} {hhmm}", "%Y-%m-%d %H:%M")


@router.callback_query(F.data == "my_shifts")
async def show_my_shifts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    client = get_client(user_id)
    worker = get_worker(user_id)

    if client:
        shifts = list_shifts_for_client(user_id)
        if not shifts:
            await callback.message.edit_text("📅 У вас пока нет смен.")
            await callback.answer()
            return

        text = "📅 *Ваши смены:*\n\n"
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
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

    elif worker:
        shifts = list_shifts_for_worker(user_id)
        if not shifts:
            await callback.message.edit_text("📅 У вас пока нет назначенных смен.")
            await callback.answer()
            return

        text = "📅 *Ваши смены:*\n\n"
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
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

    else:
        await callback.message.edit_text(
            "Не удалось определить роль. Нажмите /start и завершите регистрацию."
        )
    await callback.answer()


@router.callback_query(F.data == "my_projects")
async def my_projects(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    projects = list_projects_for_client(user_id)
    if not projects:
        await callback.message.edit_text(
            "📋 Проектов пока нет. Администратор создаёт проект и привязывает его к вам.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    text = "📋 *Ваши проекты:*\n\nСмены — в «Мои смены». Здесь — всё по проекту: список смен и общий чат.\n\n"
    rows = []
    for p in projects:
        text += f"• #{p[0]} — {p[1]}\n"
        rows.append(
            [InlineKeyboardButton(text=f"📌 Экран проекта #{p[0]}", callback_data=f"project_hub_{p[0]}")]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "create_project")
async def create_project_client_stub(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "➕ Проект создаёт *администратор* (/admin → «Создать проект») и выбирает вас как заказчика.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
        ),
        parse_mode="Markdown",
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
        await callback.message.edit_text(
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
    await callback.message.edit_text(
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
        await callback.message.edit_text("❌ Смена не найдена.")
        await callback.answer()
        return

    text = (
        f"📅 *Смена #{shift_id}*\n\n"
        f"📆 Дата: {format_date_ru(shift[2])}\n"
        f"⏰ Время: {shift[3]} — {shift[4]}\n"
        f"📍 Локация: {shift[5]}\n"
        f"💰 Ставка: {shift[6]} ₽/час\n\n"
        f"👥 *Назначенные исполнители:*\n"
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

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Вся смена здесь", callback_data=f"shift_hub_cl_{shift_id}")],
            [InlineKeyboardButton(text="📋 Поставить задачу", callback_data=f"add_task_{shift_id}")],
            [InlineKeyboardButton(text="✉️ Написать исполнителю", callback_data=f"msg_worker_pick_{shift_id}")],
            [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="📊 Отчёт", callback_data=f"report_{shift_id}")],
            [
                InlineKeyboardButton(text="📋 Проекты", callback_data="my_projects"),
                InlineKeyboardButton(text="✅ Мои задачи", callback_data="my_client_tasks"),
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("worker_shift_"))
async def worker_shift_detail(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("worker_shift_", ""))
    user_id = callback.from_user.id
    shift = get_shift(shift_id)
    assignment = get_assignment(shift_id, user_id)

    if not shift or not assignment:
        await callback.message.edit_text("❌ Смена не найдена или вы не назначены.")
        await callback.answer()
        return

    text = (
        f"📅 *Смена #{shift_id}*\n\n"
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
    keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_shift_"))
async def confirm_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("confirm_shift_", ""))
    user_id = callback.from_user.id
    confirm_assignment(shift_id, user_id)
    shift_owner = get_shift_with_owner(shift_id)
    txt = f"✅ Исполнитель {callback.from_user.full_name} подтвердил выход на смену #{shift_id}."
    try:
        await callback.bot.send_message(int(ADMIN_USER_ID), txt)
    except Exception:
        pass
    try:
        if shift_owner and shift_owner[7]:
            await callback.bot.send_message(int(shift_owner[7]), txt)
    except Exception:
        pass
    await callback.message.edit_text(
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
    await callback.message.edit_text(
        "📍 *ЧЕК-ИН*\n\nОтправьте вашу геолокацию.\n\n*Скрепка 📎 → Локация*",
        parse_mode="Markdown",
    )
    await callback.answer()


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
    now = datetime.now()
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
        "📸 *ЧЕК-ИН*\n\nОтправьте селфи на фоне объекта.\n\n*Скрепка 📎 → Камера*",
        parse_mode="Markdown",
    )


@router.message(F.photo, CheckinFlow.photo)
async def checkin_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    shift_id = data["checkin_shift_id"]
    shift_row = get_shift(int(shift_id))
    exp_lat, exp_lng, _ = _shift_geo_limits(shift_row)
    lat = data.get("checkin_lat")
    lng = data.get("checkin_lng")
    if exp_lat is not None and exp_lng is not None:
        do_checkin(
            int(shift_id),
            message.from_user.id,
            photo_id,
            float(lat) if lat is not None else None,
            float(lng) if lng is not None else None,
            1,
        )
    else:
        do_checkin(
            int(shift_id),
            message.from_user.id,
            photo_id,
            float(lat) if lat is not None else None,
            float(lng) if lng is not None else None,
            None,
        )
    shift_owner = get_shift_with_owner(int(shift_id))
    if shift_owner:
        dt_start = _shift_start(shift_owner)
        if datetime.now() > dt_start:
            mark_assignment_event_by_shift_worker(int(shift_id), message.from_user.id, "late_checkin_notified_at")
            delay_min = int((datetime.now() - dt_start).total_seconds() // 60)
            late_text = (
                f"⚠️ Исполнитель {message.from_user.full_name} отметил чек-ин по смене #{shift_id} "
                f"с опозданием на {max(delay_min, 1)} мин."
            )
            try:
                await message.bot.send_message(int(ADMIN_USER_ID), late_text)
                client_id = shift_owner[7]
                if client_id:
                    await message.bot.send_message(int(client_id), late_text)
            except Exception:
                pass
    await message.answer("✅ *Чек-ин выполнен!* Удачной смены! 🚀", parse_mode="Markdown")
    await state.clear()


@router.callback_query(F.data.startswith("checkout_"))
async def checkout_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("checkout_", ""))
    await state.update_data(checkout_shift_id=shift_id)
    await state.set_state(CheckoutFlow.geo)
    await callback.message.edit_text(
        "📍 *ЧЕК-АУТ*\n\nОтправьте финальную геолокацию.\n\n*Скрепка 📎 → Локация*",
        parse_mode="Markdown",
    )
    await callback.answer()


@router.message(F.location, CheckoutFlow.geo)
async def checkout_geo_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("checkout_shift_id")
    shift = get_shift(int(shift_id)) if shift_id else None
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
        "📸 *ЧЕК-АУТ*\n\n"
        "Отправьте селфи на фоне объекта (как при чек-ине).\n\n"
        "*Скрепка 📎 → Камера*",
        parse_mode="Markdown",
    )

@router.message(F.photo, CheckoutFlow.photo)
async def checkout_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    shift_id = data["checkout_shift_id"]
    do_checkout(shift_id, message.from_user.id, photo_id)
    await message.answer("✅ *Смена завершена!* Спасибо за работу!", parse_mode="Markdown")
    await state.clear()


@router.message(F.text, CheckoutFlow.photo)
async def checkout_skip_photo(message: types.Message, state: FSMContext):
    await message.answer(
        "Для чек-аута нужно фото (селфи). Отправьте снимок — так же, как при чек-ине."
    )


@router.callback_query(F.data.startswith("forgot_close_"))
async def forgot_close_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("forgot_close_", ""))
    do_checkout(shift_id, callback.from_user.id, None)
    await callback.message.edit_text("✅ Смена закрыта. Спасибо!")
    await callback.answer()


@router.callback_query(F.data.startswith("forgot_extend_"))
async def forgot_extend_shift(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("forgot_extend_", ""))
    await state.update_data(extend_shift_id=shift_id)
    await state.set_state(ShiftExtensionFlow.minutes)
    await callback.message.answer("⏱ На сколько минут продлить смену? Введите число, например `60`.", parse_mode="Markdown")
    await callback.answer()


@router.message(ShiftExtensionFlow.minutes)
async def forgot_extend_minutes(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Введите число минут, например `60`.", parse_mode="Markdown")
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
    await message.bot.send_message(
        int(ADMIN_USER_ID),
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
    if callback.from_user.id != int(ADMIN_USER_ID):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, _, _, shift_raw, worker_raw, min_raw = callback.data.split("_", 5)
    shift_id = int(shift_raw)
    worker_id = int(worker_raw)
    minutes = int(min_raw)
    resolve_extension_request(shift_id, worker_id, approved=True)
    extend_shift_end_time(shift_id, minutes)
    await callback.bot.send_message(worker_id, f"✅ Продление смены #{shift_id} на {minutes} минут одобрено администратором.")
    await callback.message.edit_text(f"✅ Продление по смене #{shift_id} одобрено.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_ext_no_"))
async def admin_extension_reject(callback: types.CallbackQuery):
    if callback.from_user.id != int(ADMIN_USER_ID):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    _, _, _, shift_raw, worker_raw, min_raw = callback.data.split("_", 5)
    shift_id = int(shift_raw)
    worker_id = int(worker_raw)
    minutes = int(min_raw)
    resolve_extension_request(shift_id, worker_id, approved=False)
    do_checkout(shift_id, worker_id, None)
    await callback.bot.send_message(worker_id, f"❌ Продление на {minutes} минут отклонено. Смена #{shift_id} закрыта.")
    await callback.message.edit_text(f"✅ Продление по смене #{shift_id} отклонено, смена закрыта.")
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
        await callback.message.edit_text(
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
    await callback.message.edit_text(
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
        await message.bot.send_message(
            worker_id,
            "📩 *Сообщение от заказчика*\n\n"
            f"Смена #{shift_id}\n"
            f"От: {sender}\n\n"
            f"{text}",
            parse_mode="Markdown",
        )
        await message.answer("✅ Сообщение отправлено исполнителю.")
    except Exception:
        await message.answer("❌ Не удалось отправить сообщение исполнителю.")
    await state.clear()


@router.callback_query(F.data.startswith("report_"))
async def show_shift_report(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("report_", ""))
    user_id = callback.from_user.id
    if not get_client(user_id) or not client_owns_shift(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    report = get_shift_report(shift_id)
    if not report["shift"]:
        await callback.message.edit_text("❌ Смена не найдена.")
        await callback.answer()
        return

    shift = report["shift"]
    text = (
        f"📊 *ОТЧЁТ ПО СМЕНЕ #{shift_id}*\n\n"
        f"📆 {format_date_ru(shift[2])} | {shift[3]}-{shift[4]}\n📍 {shift[5]}\n💰 Ставка: {shift[6]} ₽/час\n\n👥 *ИСПОЛНИТЕЛИ:*\n"
    )
    total_payment = 0
    for a in report["assignments"]:
        hours = float(a[9] or 0)
        payment = float(a[10] or 0)
        total_payment += payment
        status = "✅" if a[A_STATUS] == "checked_out" else "⏳"
        name = _assign_worker_name(a)
        text += f"{status} {name}: {hours:.1f} ч, {payment:.0f} ₽\n"
    text += f"\n💰 *ИТОГО:* {total_payment:.0f} ₽"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")]
        ]
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()
