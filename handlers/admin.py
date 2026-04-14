# handlers/admin.py
from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_ID
from states import ProjectCreation, ShiftAdminLine
from db import (
    create_project,
    create_shift,
    get_workers,
    assign_worker,
    list_clients,
    delete_client_cascade,
    list_projects_admin,
    list_shifts_admin,
)

router = Router()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_USER_ID


def admin_only(func):
    async def wrapper(event, *args, **kwargs):
        user_id = event.from_user.id
        if not is_admin(user_id):
            if hasattr(event, "answer") and callable(getattr(event, "answer")):
                await event.answer("⛔ У вас нет прав.", show_alert=True)
            elif hasattr(event, "message") and event.message:
                await event.message.answer("⛔ У вас нет прав.")
            return
        kwargs.pop("dispatcher", None)
        return await func(event, *args, **kwargs)

    return wrapper


def _admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Исполнители", callback_data="admin_workers")],
            [InlineKeyboardButton(text="🏢 Заказчики", callback_data="admin_clients")],
            [InlineKeyboardButton(text="➕ Создать проект", callback_data="admin_create_project")],
            [InlineKeyboardButton(text="📅 Создать смену", callback_data="admin_create_shift")],
            [InlineKeyboardButton(text="👥 Назначить на смену", callback_data="admin_assign")],
        ]
    )


@router.message(Command("admin"))
@admin_only
async def admin_panel(message: types.Message):
    await message.answer(
        "🔐 *Админ-панель DEMO*\n\nВыберите действие:",
        reply_markup=_admin_main_keyboard(),
        parse_mode="Markdown",
    )


@router.callback_query(F.data == "admin_workers")
@admin_only
async def show_workers(callback: types.CallbackQuery):
    workers = get_workers()
    if not workers:
        await callback.message.edit_text("📋 Нет зарегистрированных исполнителей.")
        await callback.answer()
        return
    text = "📋 *Исполнители:*\n\n"
    for w in workers:
        text += f"🆔 `{w[0]}` — {w[1]} | {w[2]} | {w[3]}\n"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "admin_clients")
@admin_only
async def admin_list_clients(callback: types.CallbackQuery):
    clients = list_clients()
    if not clients:
        await callback.message.edit_text(
            "🏢 Заказчиков пока нет. Пусть пользователь нажмёт «Я ЗАКАЗЧИК» в /start.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "🏢 *Заказчики (тестовые можно удалить):*\n\n"
    for c in clients:
        uid, company, contact, phone = c[0], c[1] or "—", c[2] or "—", c[3] or "—"
        text += f"🆔 `{uid}` — {company} / {contact} / {phone}\n"
        if int(uid) == int(ADMIN_USER_ID):
            text += "   _(это ваш admin-аккаунт — не удаляйте через бота, если сами заказчик)_\n"
        else:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"🗑 Удалить {contact or uid}",
                        callback_data=f"admin_delclient_{uid}",
                    )
                ]
            )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_delclient_"))
@admin_only
async def admin_delete_client(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_delclient_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    cid = int(raw)
    if cid == int(ADMIN_USER_ID):
        await callback.answer("Нельзя удалить свой admin-id как заказчика из этой кнопки.", show_alert=True)
        return
    delete_client_cascade(cid)
    await callback.message.edit_text(
        f"✅ Заказчик `{cid}` и связанные проекты/смены удалены из DEMO-БД.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 К списку", callback_data="admin_clients")]]
        ),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data == "admin_create_project")
@admin_only
async def admin_create_project_pick_client(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    clients = list_clients()
    if not clients:
        await callback.message.edit_text(
            "❌ Нет ни одного заказчика. Сначала пусть пользователь зарегистрируется как заказчик в /start.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    for c in clients:
        uid, company, contact = c[0], c[1] or "Компания", c[2] or "Контакт"
        label = f"{company[:18]} — {contact[:12]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"proj_client_{uid}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        "➕ *Новый проект*\n\nВыберите заказчика (владельца проекта):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("proj_client_"))
@admin_only
async def admin_create_project_ask_name(callback: types.CallbackQuery, state: FSMContext):
    uid = int(callback.data.replace("proj_client_", ""))
    await state.update_data(project_client_id=uid)
    await state.set_state(ProjectCreation.enter_name)
    await callback.message.answer(
        f"Введите *название проекта* для заказчика `{uid}` одним сообщением:",
        parse_mode="Markdown",
    )
    await callback.answer()


@router.message(ProjectCreation.enter_name)
@admin_only
async def admin_create_project_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    client_id = data.get("project_client_id")
    if not client_id:
        await message.answer("❌ Сессия сброшена. Начните снова: /admin → Создать проект.")
        await state.clear()
        return
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer("❌ Слишком короткое название.")
        return
    project_id = create_project(name, int(client_id))
    await state.clear()
    await message.answer(f"✅ Проект создан. ID: `{project_id}`, заказчик: `{client_id}`", parse_mode="Markdown")
    await admin_panel(message)


@router.callback_query(F.data == "admin_create_shift")
@admin_only
async def admin_create_shift_list_projects(callback: types.CallbackQuery):
    projects = list_projects_admin(20)
    if not projects:
        await callback.message.edit_text(
            "❌ Нет проектов. Создайте проект.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{p[1][:24]} ({p[3] or '—'})",
                    callback_data=f"shift_project_{p[0]}",
                )
            ]
            for p in projects
        ]
        + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
    )
    await callback.message.edit_text("📅 Выберите проект:", reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("shift_project_"))
@admin_only
async def admin_create_shift_form(callback: types.CallbackQuery, state: FSMContext):
    project_id = int(callback.data.replace("shift_project_", ""))
    await state.update_data(shift_project_id=project_id)
    await state.set_state(ShiftAdminLine.line)
    await callback.message.edit_text(
        "📅 Введите данные смены *одной строкой*:\n"
        "`ДД.ММ.ГГГГ | ЧЧ:ММ | ЧЧ:ММ | Адрес | Ставка_₽_час`\n\n"
        "*Пример:* `15.05.2026 | 10:00 | 18:00 | ТЦ Европейский | 500`",
        parse_mode="Markdown",
    )
    await callback.answer()


@router.message(ShiftAdminLine.line)
@admin_only
async def admin_create_shift_finish(message: types.Message, state: FSMContext):
    try:
        parts = [p.strip() for p in message.text.split("|")]
        if len(parts) < 4:
            raise ValueError("Нужно минимум 4 поля через |")
        date_str, start_time, end_time, location = parts[:4]
        rate = int(parts[4]) if len(parts) > 4 else 500
        data = await state.get_data()
        project_id = data.get("shift_project_id")
        if not project_id:
            await message.answer("❌ Сессия сброшена. Начните снова с /admin.")
            await state.clear()
            return
        shift_id = create_shift(
            int(project_id),
            {
                "date": date_str,
                "start_time": start_time,
                "end_time": end_time,
                "location": location,
                "rate": rate,
            },
        )
        await message.answer(f"✅ Смена создана! ID: `{shift_id}`", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    await state.clear()
    await admin_panel(message)


@router.callback_query(F.data == "admin_assign")
@admin_only
async def admin_assign_list_shifts(callback: types.CallbackQuery):
    shifts = list_shifts_admin(20)
    if not shifts:
        await callback.message.edit_text(
            "❌ Нет смен.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{s[1]} {s[2]}-{s[3]} | {s[4]}",
                    callback_data=f"assign_shift_{s[0]}",
                )
            ]
            for s in shifts
        ]
        + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
    )
    await callback.message.edit_text("📅 Выберите смену:", reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("assign_shift_"))
@admin_only
async def admin_assign_list_workers(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("assign_shift_", ""))
    workers = get_workers()
    if not workers:
        await callback.message.edit_text("❌ Нет исполнителей.")
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{w[1]} ({w[3]})", callback_data=f"do_assign_{shift_id}_{w[0]}")]
            for w in workers
        ]
        + [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_assign")]]
    )
    await callback.message.edit_text("👥 Выберите исполнителя:", reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("do_assign_"))
@admin_only
async def admin_do_assign(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    shift_id = int(parts[2])
    worker_id = int(parts[3])
    assign_worker(shift_id, worker_id)
    await callback.message.edit_text(f"✅ Исполнитель `{worker_id}` назначен на смену #{shift_id}")
    await callback.answer()


@router.callback_query(F.data == "admin_back")
@admin_only
async def admin_back(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "🔐 *Админ-панель DEMO*\n\nВыберите действие:",
        reply_markup=_admin_main_keyboard(),
        parse_mode="Markdown",
    )
    await callback.answer()
