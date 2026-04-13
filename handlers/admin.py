# handlers/admin.py
from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_ID
from states import ProjectCreation
from db import (
    create_project, create_shift,
    get_workers, assign_worker
)

router = Router()

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_USER_ID

def admin_only(func):
    async def wrapper(event, *args, **kwargs):
        user_id = event.from_user.id if hasattr(event, 'from_user') else event.message.from_user.id
        if not is_admin(user_id):
            if hasattr(event, 'answer'):
                await event.answer("⛔ У вас нет прав.", show_alert=True)
            else:
                await event.message.answer("⛔ У вас нет прав.")
            return
        return await func(event, *args, **kwargs)
    return wrapper

# ========== АДМИН-ПАНЕЛЬ ==========
@router.message(Command("admin"))
@admin_only
async def admin_panel(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список исполнителей", callback_data="admin_workers")],
        [InlineKeyboardButton(text="➕ Создать проект", callback_data="admin_create_project")],
        [InlineKeyboardButton(text="📅 Создать смену", callback_data="admin_create_shift")],
        [InlineKeyboardButton(text="👥 Назначить на смену", callback_data="admin_assign")],
    ])
    await message.answer("🔐 *Админ-панель*\n\nВыберите действие:", reply_markup=keyboard, parse_mode="Markdown")

# ========== СПИСОК ИСПОЛНИТЕЛЕЙ ==========
@router.callback_query(F.data == "admin_workers")
@admin_only
async def show_workers(callback: types.CallbackQuery):
    workers = get_workers()
    if not workers:
        await callback.message.edit_text("📋 Нет зарегистрированных исполнителей.")
        await callback.answer()
        return
    text = "📋 *Список исполнителей:*\n\n"
    for w in workers:
        text += f"🆔 `{w[0]}` — {w[1]} | {w[2]} | {w[3]}\n"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

# ========== СОЗДАНИЕ ПРОЕКТА ==========
@router.callback_query(F.data == "admin_create_project")
@admin_only
async def admin_create_project_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("➕ Введите название проекта:")
    await state.set_state(ProjectCreation.name)
    await callback.answer()

@router.message(ProjectCreation.name)
@admin_only
async def admin_create_project_finish(message: types.Message, state: FSMContext):
    project_id = create_project(message.text.strip(), ADMIN_USER_ID)
    await message.answer(f"✅ Проект создан! ID: {project_id}")
    await state.clear()
    await admin_panel(message)

# ========== СОЗДАНИЕ СМЕНЫ ==========
@router.callback_query(F.data == "admin_create_shift")
@admin_only
async def admin_create_shift_list_projects(callback: types.CallbackQuery):
    import sqlite3
    conn = sqlite3.connect("promostaff_demo.db")
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM projects ORDER BY created_at DESC LIMIT 10")
    projects = cur.fetchall()
    conn.close()
    if not projects:
        await callback.message.edit_text("❌ Сначала создайте проект.")
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=p[1], callback_data=f"shift_project_{p[0]}")] for p in projects
    ] + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]])
    await callback.message.edit_text("📅 Выберите проект:", reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("shift_project_"))
@admin_only
async def admin_create_shift_form(callback: types.CallbackQuery, state: FSMContext):
    project_id = int(callback.data.replace("shift_project_", ""))
    await state.update_data(project_id=project_id)
    await state.set_state("shift_data")
    await callback.message.edit_text(
        "📅 Введите данные смены:\n"
        "`ДД.ММ.ГГГГ | ЧЧ:ММ | ЧЧ:ММ | Адрес | Ставка`\n\n"
        "*Пример:* `15.05.2026 | 10:00 | 18:00 | ТЦ Европейский | 500`",
        parse_mode="Markdown"
    )
    await callback.answer()

@router.message(F.text, StateFilter("shift_data"))
@admin_only
async def admin_create_shift_finish(message: types.Message, state: FSMContext):
    try:
        parts = [p.strip() for p in message.text.split("|")]
        date_str, start_time, end_time, location = parts[:4]
        rate = int(parts[4]) if len(parts) > 4 else 500
        data = await state.get_data()
        project_id = data['project_id']
        shift_id = create_shift(project_id, {
            'date': date_str, 'start_time': start_time, 'end_time': end_time,
            'location': location, 'rate': rate
        })
        await message.answer(f"✅ Смена создана! ID: {shift_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    await state.clear()
    await admin_panel(message)

# ========== НАЗНАЧЕНИЕ НА СМЕНУ ==========
@router.callback_query(F.data == "admin_assign")
@admin_only
async def admin_assign_list_shifts(callback: types.CallbackQuery):
    import sqlite3
    conn = sqlite3.connect("promostaff_demo.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, s.shift_date, s.start_time, s.end_time, p.name 
        FROM shifts s JOIN projects p ON s.project_id = p.id
        ORDER BY s.shift_date DESC LIMIT 10
    """)
    shifts = cur.fetchall()
    conn.close()
    if not shifts:
        await callback.message.edit_text("❌ Нет доступных смен.")
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{s[1]} {s[2]}-{s[3]} | {s[4]}", callback_data=f"assign_shift_{s[0]}")] for s in shifts
    ] + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]])
    await callback.message.edit_text("📅 Выберите смену:", reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("assign_shift_"))
@admin_only
async def admin_assign_list_workers(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("assign_shift_", ""))
    workers = get_workers()
    if not workers:
        await callback.message.edit_text("❌ Нет зарегистрированных исполнителей.")
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{w[1]} ({w[3]})", callback_data=f"do_assign_{shift_id}_{w[0]}")] for w in workers
    ] + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_assign")]])
    await callback.message.edit_text("👥 Выберите исполнителя:", reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("do_assign_"))
@admin_only
async def admin_do_assign(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    shift_id = int(parts[2])
    worker_id = int(parts[3])
    assign_worker(shift_id, worker_id)
    await callback.message.edit_text(f"✅ Исполнитель назначен на смену #{shift_id}")
    await callback.answer()

# ========== НАВИГАЦИЯ ==========
@router.callback_query(F.data == "admin_back")
@admin_only
async def admin_back(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список исполнителей", callback_data="admin_workers")],
        [InlineKeyboardButton(text="➕ Создать проект", callback_data="admin_create_project")],
        [InlineKeyboardButton(text="📅 Создать смену", callback_data="admin_create_shift")],
        [InlineKeyboardButton(text="👥 Назначить на смену", callback_data="admin_assign")],
    ])
    await callback.message.edit_text("🔐 *Админ-панель*\n\nВыберите действие:", reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

# ========== ОБРАБОТЧИКИ ДЛЯ ЗАКАЗЧИКА ==========
@router.callback_query(F.data == "my_projects")
async def my_projects(callback: types.CallbackQuery):
    await callback.message.edit_text("📋 *Проекты*\n\nФункция в разработке.", parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data == "create_project")
async def create_project_client(callback: types.CallbackQuery):
    await callback.message.edit_text("➕ *Создание проекта*\n\nОбратитесь к администратору.", parse_mode="Markdown")
    await callback.answer()
