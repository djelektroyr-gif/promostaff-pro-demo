# handlers/admin.py
import inspect
import re
from functools import wraps

from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_ID
from states import ProjectCreation, ShiftCreation, ShiftAdminLine
from db import (
    create_project,
    create_shift,
    get_workers,
    get_workers_assignable,
    get_worker_assignment_stats,
    get_worker_status_counts,
    set_worker_status,
    get_admin_metrics,
    list_open_shifts_admin,
    close_shift_safe,
    delete_shift_cascade,
    list_admin_logs,
    log_admin_action,
    seed_demo_data,
    assign_worker,
    delete_worker_safe,
    delete_project_cascade,
    list_clients,
    delete_client_cascade,
    list_projects_admin,
    list_shifts_admin,
)

router = Router()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_USER_ID


def admin_only(func):
    signature = inspect.signature(func)
    accepted_kwargs = {
        name
        for name, p in signature.parameters.items()
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }

    @wraps(func)
    async def wrapper(event, *args, **kwargs):
        user = getattr(event, "from_user", None)
        user_id = getattr(user, "id", 0)
        if not is_admin(user_id):
            if hasattr(event, "answer") and callable(getattr(event, "answer")):
                await event.answer("⛔ У вас нет прав.", show_alert=True)
            elif hasattr(event, "message") and event.message:
                await event.message.answer("⛔ У вас нет прав.")
            return
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in accepted_kwargs}
        return await func(event, *args, **filtered_kwargs)

    return wrapper


def _admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Метрики", callback_data="admin_metrics")],
            [InlineKeyboardButton(text="📋 Исполнители", callback_data="admin_workers")],
            [InlineKeyboardButton(text="🗑 Удалить исполнителя", callback_data="admin_workers_delete")],
            [InlineKeyboardButton(text="🧭 Статусы исполнителей", callback_data="admin_worker_statuses")],
            [InlineKeyboardButton(text="🏢 Заказчики", callback_data="admin_clients")],
            [InlineKeyboardButton(text="➕ Создать проект", callback_data="admin_create_project")],
            [InlineKeyboardButton(text="🗂 Управление проектами", callback_data="admin_project_manage")],
            [InlineKeyboardButton(text="📅 Создать смену", callback_data="admin_create_shift")],
            [InlineKeyboardButton(text="🗓 Управление сменами", callback_data="admin_shift_manage")],
            [InlineKeyboardButton(text="👥 Назначить на смену", callback_data="admin_assign")],
            [InlineKeyboardButton(text="🧪 Генератор тест-данных", callback_data="admin_seed_data")],
            [InlineKeyboardButton(text="📝 Лог действий", callback_data="admin_logs")],
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


@router.callback_query(F.data == "admin_metrics")
@admin_only
async def admin_metrics(callback: types.CallbackQuery):
    m = get_admin_metrics()
    await callback.message.edit_text(
        "📊 *Быстрые метрики DEMO*\n\n"
        f"👷 Исполнители: *{m['workers']}*\n"
        f"🏢 Заказчики: *{m['clients']}*\n"
        f"📁 Проекты: *{m['projects']}*\n"
        f"📅 Открытые смены: *{m['open_shifts']}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
        ),
    )
    await callback.answer()


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


@router.callback_query(F.data == "admin_worker_statuses")
@admin_only
async def admin_worker_statuses_menu(callback: types.CallbackQuery):
    counts = get_worker_status_counts()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Все ({counts['all']})", callback_data="admin_workers_filter_all")],
            [InlineKeyboardButton(text=f"new ({counts['new']})", callback_data="admin_workers_filter_new")],
            [InlineKeyboardButton(text=f"reviewed ({counts['reviewed']})", callback_data="admin_workers_filter_reviewed")],
            [InlineKeyboardButton(text=f"approved ({counts['approved']})", callback_data="admin_workers_filter_approved")],
            [InlineKeyboardButton(text=f"rejected ({counts['rejected']})", callback_data="admin_workers_filter_rejected")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
        ]
    )
    await callback.message.edit_text(
        "🧭 *Статусы заявок исполнителей*\n\nВыберите фильтр.",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_workers_filter_"))
@admin_only
async def admin_workers_by_status(callback: types.CallbackQuery):
    status = callback.data.replace("admin_workers_filter_", "")
    workers = get_workers(None if status == "all" else status)
    if not workers:
        await callback.message.edit_text(
            f"📋 Нет исполнителей со статусом `{status}`.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 К фильтрам", callback_data="admin_worker_statuses")],
                ]
            ),
        )
        await callback.answer()
        return
    text = f"📋 *Исполнители ({status})*\n\n"
    rows = []
    for w in workers:
        uid, name, phone, profession, cur_status = w[0], w[1], w[2], w[3], w[4]
        text += f"🆔 `{uid}` — {name} | {profession} | статус: *{cur_status}*\n"
        rows.append([InlineKeyboardButton(text=f"⚙️ Статус {uid}", callback_data=f"admin_worker_status_{uid}")])
    rows.append([InlineKeyboardButton(text="🔙 К фильтрам", callback_data="admin_worker_statuses")])
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_worker_status_"))
@admin_only
async def admin_worker_status_pick(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_worker_status_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    worker_id = int(raw)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="new", callback_data=f"admin_worker_set_{worker_id}_new")],
            [InlineKeyboardButton(text="reviewed", callback_data=f"admin_worker_set_{worker_id}_reviewed")],
            [InlineKeyboardButton(text="approved", callback_data=f"admin_worker_set_{worker_id}_approved")],
            [InlineKeyboardButton(text="rejected", callback_data=f"admin_worker_set_{worker_id}_rejected")],
            [InlineKeyboardButton(text="🔙 К фильтрам", callback_data="admin_worker_statuses")],
        ]
    )
    await callback.message.edit_text(
        f"⚙️ Выберите новый статус для исполнителя `{worker_id}`:",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_worker_set_"))
@admin_only
async def admin_worker_status_set(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_worker_set_", "")
    parts = raw.split("_", 1)
    if len(parts) != 2 or not parts[0].isdigit():
        await callback.answer()
        return
    worker_id = int(parts[0])
    new_status = parts[1]
    ok = set_worker_status(worker_id, new_status)
    if ok:
        log_admin_action(callback.from_user.id, "set_worker_status", "worker", worker_id, new_status)
        await callback.message.edit_text(
            f"✅ Статус исполнителя `{worker_id}` обновлён: *{new_status}*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 К фильтрам", callback_data="admin_worker_statuses")]]
            ),
        )
    else:
        await callback.message.edit_text(
            "❌ Не удалось обновить статус.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 К фильтрам", callback_data="admin_worker_statuses")]]
            ),
        )
    await callback.answer()


@router.callback_query(F.data == "admin_workers_delete")
@admin_only
async def admin_workers_delete_menu(callback: types.CallbackQuery):
    workers = get_workers()
    if not workers:
        await callback.message.edit_text(
            "🗑 Нет исполнителей для удаления.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return

    rows = []
    text = "🗑 *Удаление исполнителя*\n\nВыберите исполнителя:\n"
    for w in workers:
        uid, full_name = int(w[0]), w[1] or f"ID {w[0]}"
        stats = get_worker_assignment_stats(uid)
        text += (
            f"\n• `{uid}` — {full_name}\n"
            f"  Назначений: {stats['assignments_total']}, открытых задач: {stats['open_tasks']}"
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 {full_name[:18]} ({uid})",
                    callback_data=f"admin_worker_delask_{uid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_worker_delask_"))
@admin_only
async def admin_worker_delete_confirm(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_worker_delask_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    worker_id = int(raw)
    worker = next((w for w in get_workers() if int(w[0]) == worker_id), None)
    worker_name = worker[1] if worker else f"ID {worker_id}"
    stats = get_worker_assignment_stats(worker_id)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_worker_deldo_{worker_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_workers_delete")],
        ]
    )
    await callback.message.edit_text(
        "⚠️ *Подтверждение удаления*\n\n"
        f"Исполнитель: `{worker_id}` — {worker_name}\n"
        f"Назначений будет удалено: *{stats['assignments_total']}*\n"
        f"Открытых задач будет отвязано: *{stats['open_tasks']}*\n\n"
        "Продолжить?",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_worker_deldo_"))
@admin_only
async def admin_worker_delete_do(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_worker_deldo_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    worker_id = int(raw)
    result = delete_worker_safe(worker_id)
    if not result["deleted"]:
        await callback.message.edit_text(
            f"❌ Исполнитель `{worker_id}` не найден.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 К удалению", callback_data="admin_workers_delete")]]
            ),
            parse_mode="Markdown",
        )
        await callback.answer()
        return
    log_admin_action(
        callback.from_user.id,
        "delete_worker_safe",
        "worker",
        worker_id,
        f"assignments={result['assignments_deleted']};tasks={result['tasks_unassigned']}",
    )
    await callback.message.edit_text(
        "✅ Исполнитель удалён безопасно.\n\n"
        f"ID: `{worker_id}`\n"
        f"Удалено назначений: *{result['assignments_deleted']}*\n"
        f"Отвязано задач: *{result['tasks_unassigned']}*",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 К удалению", callback_data="admin_workers_delete")],
                [InlineKeyboardButton(text="🏠 В админ-панель", callback_data="admin_back")],
            ]
        ),
        parse_mode="Markdown",
    )
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
    log_admin_action(callback.from_user.id, "delete_client_cascade", "client", cid, "cascade")
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
    log_admin_action(message.from_user.id, "create_project", "project", project_id, f"client_id={client_id}")
    await state.clear()
    await message.answer(f"✅ Проект создан. ID: `{project_id}`, заказчик: `{client_id}`", parse_mode="Markdown")
    await admin_panel(message)


@router.callback_query(F.data == "admin_project_manage")
@admin_only
async def admin_project_manage(callback: types.CallbackQuery):
    projects = list_projects_admin(50)
    if not projects:
        await callback.message.edit_text(
            "📁 Проектов пока нет.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "🗂 *Управление проектами*\n\n"
    for p in projects:
        pid, name, client_id, company_name, contact_name = p
        text += f"• #{pid} — {name} | клиент: {company_name or contact_name or client_id}\n"
        rows.append([InlineKeyboardButton(text=f"🗑 Удалить проект #{pid}", callback_data=f"admin_project_delask_{pid}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_project_delask_"))
@admin_only
async def admin_project_delete_confirm(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_project_delask_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    pid = int(raw)
    await callback.message.edit_text(
        f"⚠️ Удалить проект #{pid} и каскадно удалить все его смены/назначения/задачи/чат?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Да, удалить проект", callback_data=f"admin_project_deldo_{pid}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_project_manage")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_project_deldo_"))
@admin_only
async def admin_project_delete_do(callback: types.CallbackQuery):
    raw = callback.data.replace("admin_project_deldo_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    pid = int(raw)
    result = delete_project_cascade(pid)
    if result["deleted"]:
        log_admin_action(
            callback.from_user.id,
            "delete_project_cascade",
            "project",
            pid,
            f"shifts={result['shifts']};assignments={result['assignments']};tasks={result['tasks']};chat={result['chat_messages']}",
        )
        await callback.answer("Проект удалён", show_alert=False)
    else:
        await callback.answer("Проект не найден", show_alert=True)
    await admin_project_manage(callback)


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
    await state.clear()
    await state.update_data(shift_project_id=project_id)
    await state.set_state(ShiftCreation.date)
    await callback.message.edit_text(
        "📅 *Создание смены (шаг 1/5)*\n\n"
        "Введите дату в формате `ДД.ММ.ГГГГ`.\n"
        "Пример: `15.05.2026`",
        parse_mode="Markdown",
    )
    await callback.answer()


def _is_hhmm(value: str) -> bool:
    return bool(re.match(r"^(?:[01]\d|2[0-3]):[0-5]\d$", value))


def _parse_coords(value: str) -> tuple[float, float] | None:
    raw = (value or "").strip().replace(" ", "")
    if raw in ("0", "-", "skip"):
        return None
    if "," not in raw:
        raise ValueError("Формат: широта,долгота")
    lat_raw, lng_raw = raw.split(",", 1)
    lat = float(lat_raw)
    lng = float(lng_raw)
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise ValueError("Координаты вне диапазона")
    return lat, lng


@router.message(ShiftCreation.date)
@admin_only
async def admin_create_shift_date(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    try:
        # Валидация даты через общий парсер db.py
        from db import normalize_shift_date

        normalize_shift_date(raw)
    except Exception:
        await message.answer("❌ Неверная дата. Используйте формат `ДД.ММ.ГГГГ`, например `15.05.2026`.", parse_mode="Markdown")
        return
    await state.update_data(shift_date=raw)
    await state.set_state(ShiftCreation.start_time)
    await message.answer(
        "⏰ *Шаг 2/5*\nВведите время начала в формате `ЧЧ:ММ`.\nПример: `10:00`",
        parse_mode="Markdown",
    )


@router.message(ShiftCreation.start_time)
@admin_only
async def admin_create_shift_start_time(message: types.Message, state: FSMContext):
    start_time = (message.text or "").strip()
    if not _is_hhmm(start_time):
        await message.answer("❌ Неверный формат времени. Пример: `10:00`", parse_mode="Markdown")
        return
    await state.update_data(start_time=start_time)
    await state.set_state(ShiftCreation.end_time)
    await message.answer(
        "⏱ *Шаг 3/7*\nВведите время окончания в формате `ЧЧ:ММ`.\nПример: `18:00`",
        parse_mode="Markdown",
    )


@router.message(ShiftCreation.end_time)
@admin_only
async def admin_create_shift_end_time(message: types.Message, state: FSMContext):
    end_time = (message.text or "").strip()
    if not _is_hhmm(end_time):
        await message.answer("❌ Неверный формат времени. Пример: `18:00`", parse_mode="Markdown")
        return
    data = await state.get_data()
    start_time = (data.get("start_time") or "").strip()
    if start_time == end_time:
        await message.answer("❌ Время окончания не должно совпадать со временем начала.")
        return
    await state.update_data(end_time=end_time)
    await state.set_state(ShiftCreation.location)
    await message.answer("📍 *Шаг 4/7*\nВведите адрес/локацию смены.")


@router.message(ShiftCreation.location)
@admin_only
async def admin_create_shift_location(message: types.Message, state: FSMContext):
    location = (message.text or "").strip()
    if len(location) < 5:
        await message.answer("❌ Слишком короткий адрес. Введите локацию подробнее.")
        return
    await state.update_data(location=location)
    await state.set_state(ShiftCreation.coords)
    await message.answer(
        "🧭 *Шаг 5/7*\n"
        "Введите координаты площадки в формате `55.7522,37.6156`.\n"
        "Если контроль гео не нужен, отправьте `0`.",
        parse_mode="Markdown",
    )


@router.message(ShiftCreation.coords)
@admin_only
async def admin_create_shift_coords(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    try:
        coords = _parse_coords(raw)
    except Exception:
        await message.answer(
            "❌ Неверный формат координат. Пример: `55.7522,37.6156` или `0` чтобы пропустить.",
            parse_mode="Markdown",
        )
        return
    if coords is None:
        await state.update_data(expected_lat=None, expected_lng=None, checkin_radius_m=None)
        await state.set_state(ShiftCreation.rate)
        await message.answer(
            "💰 *Шаг 7/7*\nВведите ставку в рублях за час (только число).\nПример: `500`",
            parse_mode="Markdown",
        )
        return
    await state.update_data(expected_lat=coords[0], expected_lng=coords[1])
    await state.set_state(ShiftCreation.radius)
    await message.answer(
        "📐 *Шаг 6/7*\nВведите радиус допуска в метрах (например `300`).",
        parse_mode="Markdown",
    )


@router.message(ShiftCreation.radius)
@admin_only
async def admin_create_shift_radius(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("❌ Радиус должен быть числом, например `300`.", parse_mode="Markdown")
        return
    radius = int(raw)
    if radius < 30 or radius > 3000:
        await message.answer("❌ Радиус должен быть в диапазоне 30..3000 метров.")
        return
    await state.update_data(checkin_radius_m=radius)
    await state.set_state(ShiftCreation.rate)
    await message.answer(
        "💰 *Шаг 7/7*\nВведите ставку в рублях за час (только число).\nПример: `500`",
        parse_mode="Markdown",
    )


@router.message(ShiftCreation.rate)
@admin_only
async def admin_create_shift_finish(message: types.Message, state: FSMContext):
    rate_raw = (message.text or "").strip()
    if not rate_raw.isdigit():
        await message.answer("❌ Ставка должна быть числом, например `500`.", parse_mode="Markdown")
        return
    rate = int(rate_raw)
    if rate <= 0 or rate > 10000:
        await message.answer("❌ Ставка должна быть в диапазоне 1..10000.")
        return
    try:
        data = await state.get_data()
        project_id = data.get("shift_project_id")
        if not project_id:
            await message.answer("❌ Сессия сброшена. Начните снова с /admin.")
            await state.clear()
            return
        shift_id = create_shift(
            int(project_id),
            {
                "date": data.get("shift_date"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "location": data.get("location"),
                "rate": rate,
                "expected_lat": data.get("expected_lat"),
                "expected_lng": data.get("expected_lng"),
                "checkin_radius_m": data.get("checkin_radius_m") or 300,
            },
        )
        log_admin_action(message.from_user.id, "create_shift", "shift", shift_id, f"project_id={project_id}")
        coords_line = "без контроля гео"
        if data.get("expected_lat") is not None and data.get("expected_lng") is not None:
            coords_line = (
                f"`{data.get('expected_lat')},{data.get('expected_lng')}`\n"
                f"Радиус: `{data.get('checkin_radius_m') or 300} м`"
            )
        await message.answer(
            "✅ Смена создана!\n\n"
            f"ID: `{shift_id}`\n"
            f"Дата: `{data.get('shift_date')}`\n"
            f"Время: `{data.get('start_time')} - {data.get('end_time')}`\n"
            f"Локация: {data.get('location')}\n"
            f"Координаты/геоконтроль: {coords_line}\n"
            f"Ставка: `{rate}` ₽/час",
            parse_mode="Markdown",
        )
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
    workers = get_workers_assignable()
    if not workers:
        await callback.message.edit_text("❌ Нет доступных исполнителей (кроме `rejected`).", parse_mode="Markdown")
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{w[1]} ({w[3]}) [{w[4] or 'new'}]", callback_data=f"do_assign_{shift_id}_{w[0]}")]
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
    log_admin_action(callback.from_user.id, "assign_worker", "shift", shift_id, f"worker_id={worker_id}")
    shift = list(filter(lambda x: int(x[0]) == int(shift_id), list_shifts_admin(100)))
    shift_line = ""
    if shift:
        s = shift[0]
        shift_line = f"\n📅 {s[1]} {s[2]}-{s[3]} | {s[4]}"
    try:
        await callback.bot.send_message(
            worker_id,
            f"📌 Вас назначили на смену #{shift_id}.{shift_line}\n\nОткройте «Мои смены» и подтвердите выход.",
        )
    except Exception:
        pass
    await callback.message.edit_text(f"✅ Исполнитель `{worker_id}` назначен на смену #{shift_id}")
    await callback.answer()


@router.callback_query(F.data == "admin_shift_manage")
@admin_only
async def admin_shift_manage_list(callback: types.CallbackQuery):
    shifts = list_open_shifts_admin(30)
    if not shifts:
        await callback.message.edit_text(
            "📅 Нет открытых смен.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "🗓 *Управление сменами*\n\n"
    for s in shifts:
        sid, date, st, et, project_name, status = s
        text += f"• #{sid} {date} {st}-{et} | {project_name} | {status}\n"
        rows.append([InlineKeyboardButton(text=f"🛑 Закрыть #{sid}", callback_data=f"admin_shift_close_{sid}")])
        rows.append([InlineKeyboardButton(text=f"🗑 Удалить #{sid}", callback_data=f"admin_shift_delask_{sid}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_shift_close_"))
@admin_only
async def admin_shift_close(callback: types.CallbackQuery):
    sid_raw = callback.data.replace("admin_shift_close_", "")
    if not sid_raw.isdigit():
        await callback.answer()
        return
    sid = int(sid_raw)
    result = close_shift_safe(sid)
    if result["closed"]:
        log_admin_action(callback.from_user.id, "close_shift_safe", "shift", sid, result["reason"])
        await callback.answer("Смена закрыта", show_alert=False)
    else:
        await callback.answer("Смена не найдена", show_alert=True)
    await admin_shift_manage_list(callback)


@router.callback_query(F.data.startswith("admin_shift_delask_"))
@admin_only
async def admin_shift_delete_confirm(callback: types.CallbackQuery):
    sid_raw = callback.data.replace("admin_shift_delask_", "")
    if not sid_raw.isdigit():
        await callback.answer()
        return
    sid = int(sid_raw)
    await callback.message.edit_text(
        f"⚠️ Удалить смену #{sid} и каскадно удалить назначения/задачи/чат?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_shift_deldo_{sid}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_shift_manage")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_shift_deldo_"))
@admin_only
async def admin_shift_delete_do(callback: types.CallbackQuery):
    sid_raw = callback.data.replace("admin_shift_deldo_", "")
    if not sid_raw.isdigit():
        await callback.answer()
        return
    sid = int(sid_raw)
    result = delete_shift_cascade(sid)
    if result["deleted"]:
        log_admin_action(
            callback.from_user.id,
            "delete_shift_cascade",
            "shift",
            sid,
            f"assignments={result['assignments']};tasks={result['tasks']};chat={result['chat_messages']}",
        )
        await callback.answer("Смена удалена", show_alert=False)
    else:
        await callback.answer("Смена не найдена", show_alert=True)
    await admin_shift_manage_list(callback)


@router.callback_query(F.data == "admin_logs")
@admin_only
async def admin_show_logs(callback: types.CallbackQuery):
    logs = list_admin_logs(25)
    if not logs:
        await callback.message.edit_text(
            "📝 Лог действий пока пуст.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
            ),
        )
        await callback.answer()
        return
    text = "📝 *Последние действия админа:*\n\n"
    for row in logs:
        admin_uid, action, entity_type, entity_id, details, created_at = row
        text += f"• {created_at} | {action} {entity_type}#{entity_id or '-'} | {details}\n"
    await callback.message.edit_text(
        text[:3900],
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "admin_seed_data")
@admin_only
async def admin_seed_data_run(callback: types.CallbackQuery):
    stats = seed_demo_data()
    log_admin_action(callback.from_user.id, "seed_demo_data", "system", None, str(stats))
    await callback.message.edit_text(
        "🧪 Тест-данные созданы:\n\n"
        f"👷 Исполнители: {stats['workers']}\n"
        f"🏢 Заказчики: {stats['clients']}\n"
        f"📁 Проекты: {stats['projects']}\n"
        f"📅 Смены: {stats['shifts']}\n"
        f"👥 Назначения: {stats['assignments']}\n"
        f"📋 Задачи: {stats['tasks']}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]]
        ),
    )
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
