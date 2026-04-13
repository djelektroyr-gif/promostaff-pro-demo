# handlers/shifts.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
import asyncio

from config import ADMIN_USER_ID
from db import (
    get_client, get_worker, get_shifts_by_project, get_shift,
    get_shift_assignments, get_assignment,
    confirm_assignment, do_checkin, do_checkout, get_shift_report
)

router = Router()

# ========== ПРОСМОТР СМЕН (ЗАКАЗЧИК) ==========
@router.callback_query(F.data == "my_shifts")
async def show_my_shifts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    client = get_client(user_id)
    worker = get_worker(user_id)
    
    if client:
        # Для заказчика показываем все смены его проектов
        import sqlite3
        conn = sqlite3.connect("promostaff_demo.db")
        cur = conn.cursor()
        cur.execute("""
            SELECT s.id, s.shift_date, s.start_time, s.end_time, s.location, p.name, s.status
            FROM shifts s
            JOIN projects p ON s.project_id = p.id
            WHERE p.client_id = ?
            ORDER BY s.shift_date DESC
        """, (user_id,))
        shifts = cur.fetchall()
        conn.close()
        
        if not shifts:
            await callback.message.edit_text("📅 У вас пока нет смен.")
            await callback.answer()
            return
        
        text = "📅 *Ваши смены:*\n\n"
        keyboard_rows = []
        for s in shifts:
            status_emoji = "🟢" if s[6] == 'open' else "🔵" if s[6] == 'in_progress' else "⚪"
            text += f"{status_emoji} {s[1]} {s[2]}-{s[3]} | {s[5]}\n"
            keyboard_rows.append([InlineKeyboardButton(
                text=f"{s[1]} {s[2]}-{s[3]}", 
                callback_data=f"shift_detail_{s[0]}"
            )])
        
        keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    
    elif worker:
        # Для исполнителя показываем его назначения
        import sqlite3
        conn = sqlite3.connect("promostaff_demo.db")
        cur = conn.cursor()
        cur.execute("""
            SELECT s.id, s.shift_date, s.start_time, s.end_time, s.location, a.status
            FROM assignments a
            JOIN shifts s ON a.shift_id = s.id
            WHERE a.worker_id = ?
            ORDER BY s.shift_date DESC
        """, (user_id,))
        shifts = cur.fetchall()
        conn.close()
        
        if not shifts:
            await callback.message.edit_text("📅 У вас пока нет назначенных смен.")
            await callback.answer()
            return
        
        text = "📅 *Ваши смены:*\n\n"
        keyboard_rows = []
        for s in shifts:
            status_text = {
                'pending': '⏳ Ожидает',
                'confirmed': '✅ Подтверждена',
                'checked_in': '🔵 В процессе',
                'checked_out': '⚪ Завершена'
            }.get(s[5], s[5])
            text += f"{status_text}: {s[1]} {s[2]}-{s[3]}\n"
            keyboard_rows.append([InlineKeyboardButton(
                text=f"{s[1]} {s[2]}-{s[3]}", 
                callback_data=f"worker_shift_{s[0]}"
            )])
        
        keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    
    await callback.answer()

# ========== ДЕТАЛИ СМЕНЫ ДЛЯ ЗАКАЗЧИКА ==========
@router.callback_query(F.data.startswith("shift_detail_"))
async def shift_detail(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("shift_detail_", ""))
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
            'pending': '⏳ Ожидает подтверждения',
            'confirmed': '✅ Подтвердил',
            'checked_in': '🔵 На смене',
            'checked_out': '⚪ Завершил'
        }.get(a[3], a[3])
        text += f"• {a[5]}: {status_text}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")],
        [InlineKeyboardButton(text="📋 Поставить задачу", callback_data=f"add_task_{shift_id}")],
        [InlineKeyboardButton(text="📊 Отчёт", callback_data=f"report_{shift_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

# ========== ДЕТАЛИ СМЕНЫ ДЛЯ ИСПОЛНИТЕЛЯ ==========
@router.callback_query(F.data.startswith("worker_shift_"))
async def worker_shift_detail(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("worker_shift_", ""))
    user_id = callback.from_user.id
    shift = get_shift(shift_id)
    assignment = get_assignment(shift_id, user_id)
    
    if not shift or not assignment:
        await callback.message.edit_text("❌ Смена не найдена.")
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
        'pending': '⏳ Ожидает вашего подтверждения',
        'confirmed': '✅ Вы подтвердили выход',
        'checked_in': '🔵 Вы на смене',
        'checked_out': '⚪ Смена завершена'
    }.get(status, status)
    text += status_text
    
    keyboard_rows = []
    
    if status == 'pending':
        keyboard_rows.append([InlineKeyboardButton(text="✅ Подтвердить выход", callback_data=f"confirm_shift_{shift_id}")])
    elif status == 'confirmed':
        keyboard_rows.append([InlineKeyboardButton(text="✅ Чек-ин", callback_data=f"checkin_{shift_id}")])
    elif status == 'checked_in':
        keyboard_rows.append([InlineKeyboardButton(text="✅ Чек-аут", callback_data=f"checkout_{shift_id}")])
    
    keyboard_rows.append([InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")])
    keyboard_rows.append([InlineKeyboardButton(text="📋 Мои задачи", callback_data=f"tasks_{shift_id}")])
    keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

# ========== ПОДТВЕРЖДЕНИЕ ВЫХОДА ==========
@router.callback_query(F.data.startswith("confirm_shift_"))
async def confirm_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("confirm_shift_", ""))
    user_id = callback.from_user.id
    
    confirm_assignment(shift_id, user_id)
    
    # Уведомляем заказчика
    shift = get_shift(shift_id)
    if shift:
        import sqlite3
        conn = sqlite3.connect("promostaff_demo.db")
        cur = conn.cursor()
        cur.execute("SELECT client_id FROM projects WHERE id = ?", (shift[1],))
        row = cur.fetchone()
        conn.close()
        
        if row:
            worker = get_worker(user_id)
            await callback.bot.send_message(
                row[0],
                f"✅ *Исполнитель подтвердил выход*\n\n"
                f"👤 {worker[1]}\n"
                f"📅 Смена #{shift_id} ({shift[2]} {shift[3]}-{shift[4]})",
                parse_mode="Markdown"
            )
    
    await callback.message.edit_text(
        f"✅ Вы подтвердили выход на смену #{shift_id}!\n\n"
        "За 12 и 3 часа до начала вам придёт напоминание.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 К смене", callback_data=f"worker_shift_{shift_id}")]
        ])
    )
    await callback.answer()

# ========== ЧЕК-ИН ==========
@router.callback_query(F.data.startswith("checkin_"))
async def checkin_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("checkin_", ""))
    await state.update_data(checkin_shift_id=shift_id)
    
    await callback.message.edit_text(
        "📍 *ЧЕК-ИН*\n\n"
        "Отправьте вашу геолокацию для подтверждения нахождения на объекте.\n\n"
        "*Нажмите на скрепку 📎 → Локация*",
        parse_mode="Markdown"
    )
    await state.set_state("waiting_checkin_geo")
    await callback.answer()

@router.message(F.location, state="waiting_checkin_geo")
async def checkin_geo_received(message: types.Message, state: FSMContext):
    await state.update_data(checkin_lat=message.location.latitude, checkin_lng=message.location.longitude)
    
    await message.answer(
        "📸 *ЧЕК-ИН*\n\n"
        "Теперь отправьте селфи на фоне объекта.\n\n"
        "*Нажмите на скрепку 📎 → Камера*",
        parse_mode="Markdown"
    )
    await state.set_state("waiting_checkin_photo")

@router.message(F.photo, state="waiting_checkin_photo")
async def checkin_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    shift_id = data['checkin_shift_id']
    user_id = message.from_user.id
    
    do_checkin(shift_id, user_id, photo_id)
    
    await message.answer(
        "✅ *Чек-ин выполнен!*\n\n"
        "Удачной смены! 🚀\n\n"
        "Не забудьте отмечать перерывы и выполнять задачи.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="📋 Мои задачи", callback_data=f"tasks_{shift_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
    )
    
    # Уведомляем заказчика
    shift = get_shift(shift_id)
    if shift:
        import sqlite3
        conn = sqlite3.connect("promostaff_demo.db")
        cur = conn.cursor()
        cur.execute("SELECT client_id FROM projects WHERE id = ?", (shift[1],))
        row = cur.fetchone()
        conn.close()
        
        if row:
            worker = get_worker(user_id)
            await message.bot.send_message(
                row[0],
                f"✅ *Исполнитель на объекте!*\n\n"
                f"👤 {worker[1]}\n"
                f"📅 Смена #{shift_id}",
                parse_mode="Markdown"
            )
    
    await state.clear()

# ========== ЧЕК-АУТ ==========
@router.callback_query(F.data.startswith("checkout_"))
async def checkout_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("checkout_", ""))
    await state.update_data(checkout_shift_id=shift_id)
    
    await callback.message.edit_text(
        "📍 *ЧЕК-АУТ*\n\n"
        "Отправьте финальную геолокацию.\n\n"
        "*Нажмите на скрепку 📎 → Локация*",
        parse_mode="Markdown"
    )
    await state.set_state("waiting_checkout_geo")
    await callback.answer()

@router.message(F.location, state="waiting_checkout_geo")
async def checkout_geo_received(message: types.Message, state: FSMContext):
    await state.update_data(checkout_lat=message.location.latitude, checkout_lng=message.location.longitude)
    
    await message.answer(
        "📸 *ЧЕК-АУТ*\n\n"
        "Отправьте финальное фото (по желанию, отправьте `0` чтобы пропустить).\n\n"
        "*Нажмите на скрепку 📎 → Камера*",
        parse_mode="Markdown"
    )
    await state.set_state("waiting_checkout_photo")

@router.message(F.photo, state="waiting_checkout_photo")
async def checkout_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await finish_checkout(message, state, photo_id)

@router.message(F.text, state="waiting_checkout_photo")
async def checkout_skip_photo(message: types.Message, state: FSMContext):
    if message.text == "0":
        await finish_checkout(message, state, None)
    else:
        await message.answer("Отправьте фото или `0` чтобы пропустить.")

async def finish_checkout(message: types.Message, state: FSMContext, photo_id: str = None):
    data = await state.get_data()
    shift_id = data['checkout_shift_id']
    user_id = message.from_user.id
    
    do_checkout(shift_id, user_id, photo_id)
    
    # Получаем информацию о выплате
    assignment = get_assignment(shift_id, user_id)
    hours = assignment[8] if assignment and len(assignment) > 8 else 0
    payment = assignment[9] if assignment and len(assignment) > 9 else 0
    
    await message.answer(
        f"✅ *Смена завершена!*\n\n"
        f"⏱ Отработано: {hours:.1f} ч\n"
        f"💰 К выплате: {payment:.0f} ₽\n\n"
        "Спасибо за работу!",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
    )
    
    # Уведомляем заказчика
    shift = get_shift(shift_id)
    if shift:
        import sqlite3
        conn = sqlite3.connect("promostaff_demo.db")
        cur = conn.cursor()
        cur.execute("SELECT client_id FROM projects WHERE id = ?", (shift[1],))
        row = cur.fetchone()
        conn.close()
        
        if row:
            worker = get_worker(user_id)
            await message.bot.send_message(
                row[0],
                f"⚪ *Исполнитель завершил смену*\n\n"
                f"👤 {worker[1]}\n"
                f"📅 Смена #{shift_id}\n"
                f"⏱ Отработано: {hours:.1f} ч\n"
                f"💰 К выплате: {payment:.0f} ₽",
                parse_mode="Markdown"
            )
    
    await state.clear()

# ========== ОТЧЁТ ПО СМЕНЕ ==========
@router.callback_query(F.data.startswith("report_"))
async def show_shift_report(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("report_", ""))
    report = get_shift_report(shift_id)
    
    if not report['shift']:
        await callback.message.edit_text("❌ Смена не найдена.")
        await callback.answer()
        return
    
    shift = report['shift']
    text = (
        f"📊 *ОТЧЁТ ПО СМЕНЕ #{shift_id}*\n\n"
        f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n"
        f"📍 {shift[5]}\n"
        f"💰 Ставка: {shift[6]} ₽/час\n\n"
        f"👥 *ИСПОЛНИТЕЛИ:*\n"
    )
    
    total_hours = 0
    total_payment = 0
    
    for a in report['assignments']:
        hours = a[8] if a[8] else 0
        payment = a[9] if a[9] else 0
        total_hours += hours
        total_payment += payment
        
        status = "✅" if a[3] == 'checked_out' else "⏳"
        text += f"{status} {a[10]}: {hours:.1f} ч, {payment:.0f} ₽\n"
    
    text += f"\n📋 *ЗАДАЧИ:*\n"
    for t in report['tasks']:
        task_status = "✅" if t[6] == 'completed' else "⏳"
        worker_name = t[8] if t[8] else "Не назначена"
        text += f"{task_status} {t[2]}: {worker_name}\n"
    
    text += f"\n💰 *ИТОГО:* {total_payment:.0f} ₽"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()
