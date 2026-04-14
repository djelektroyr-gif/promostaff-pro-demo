# handlers/shifts.py
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
)
from states import CheckinFlow, CheckoutFlow

router = Router()

# Индексы строки assignments + JOIN workers: a[0..10] + full_name a[11], phone a[12]
A_STATUS = 3
A_FULL_NAME = 11


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
            text += f"{status_emoji} {s[1]} {s[2]}-{s[3]} | {s[5]}\n"
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{s[1]} {s[2]}-{s[3]}",
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
            text += f"{status_text}: {s[1]} {s[2]}-{s[3]}\n"
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{s[1]} {s[2]}-{s[3]}",
                        callback_data=f"worker_shift_{s[0]}",
                    )
                ]
            )
        keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

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
    text = "📋 *Ваши проекты:*\n\nСмены смотрите в «Мои смены».\n\n"
    for p in projects:
        text += f"• #{p[0]} — {p[1]}\n"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
    )
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
        f"📆 Дата: {shift[2]}\n"
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
        name = a[A_FULL_NAME] if len(a) > A_FULL_NAME else "—"
        text += f"• {name}: {status_text}\n"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Поставить задачу", callback_data=f"add_task_{shift_id}")],
            [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="📊 Отчёт", callback_data=f"report_{shift_id}")],
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
        f"📆 Дата: {shift[2]}\n"
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
    keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_shift_"))
async def confirm_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("confirm_shift_", ""))
    user_id = callback.from_user.id
    confirm_assignment(shift_id, user_id)
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
    do_checkin(shift_id, message.from_user.id, photo_id)
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
    await state.update_data(
        checkout_lat=message.location.latitude,
        checkout_lng=message.location.longitude,
    )
    await state.set_state(CheckoutFlow.photo)
    await message.answer(
        "📸 *ЧЕК-АУТ*\n\nОтправьте финальное фото (или текст `0` чтобы пропустить).",
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
    if message.text == "0":
        data = await state.get_data()
        shift_id = data["checkout_shift_id"]
        do_checkout(shift_id, message.from_user.id, None)
        await message.answer("✅ *Смена завершена!* Спасибо за работу!", parse_mode="Markdown")
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0` чтобы пропустить.")


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
        f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n📍 {shift[5]}\n💰 Ставка: {shift[6]} ₽/час\n\n👥 *ИСПОЛНИТЕЛИ:*\n"
    )
    total_payment = 0
    for a in report["assignments"]:
        hours = float(a[9] or 0)
        payment = float(a[10] or 0)
        total_payment += payment
        status = "✅" if a[A_STATUS] == "checked_out" else "⏳"
        name = a[A_FULL_NAME] if len(a) > A_FULL_NAME else "—"
        text += f"{status} {name}: {hours:.1f} ч, {payment:.0f} ₽\n"
    text += f"\n💰 *ИТОГО:* {total_payment:.0f} ₽"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")]
        ]
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()
