# handlers/chat.py
from aiogram import Router, F, types, Bot
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_IDS, is_admin_user
from db import (
    save_chat_message,
    get_chat_messages,
    get_shift,
    get_shift_assignments,
    get_worker,
    get_client,
    get_assignment,
    client_owns_shift,
    get_project,
    save_project_chat_message,
    get_project_chat_messages,
    client_owns_project,
    worker_assigned_to_project,
    get_shift_with_owner,
)
from states import ChatMessageState, ProjectChatState

from .telegram_edit import safe_edit_or_resend

router = Router()


async def _broadcast_shift_chat_message(
    bot: Bot,
    *,
    shift_id: int,
    sender_id: int,
    display_name: str,
    body: str,
) -> None:
    recipients: set[int] = set()
    shift_owner = get_shift_with_owner(shift_id)
    if shift_owner and shift_owner[7]:
        recipients.add(int(shift_owner[7]))
    for a in get_shift_assignments(shift_id):
        recipients.add(int(a[2]))
    for aid in ADMIN_USER_IDS:
        recipients.add(int(aid))
    recipients.discard(int(sender_id))

    text = (
        f"💬 Новое сообщение в чате смены #{shift_id}\n\n"
        f"{display_name}:\n{body[:800]}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть чат смены", callback_data=f"chat_{shift_id}")]]
    )
    for rid in recipients:
        try:
            await bot.send_message(int(rid), text, parse_mode=None, reply_markup=kb)
        except Exception:
            # Неподписанный/заблокированный получатель не должен ломать отправку остальным.
            continue


def get_user_display_name(user_id: int) -> str:
    if is_admin_user(int(user_id)):
        return "Координатор PROMOSTAFF"
    worker = get_worker(user_id)
    if worker:
        full_name = (worker[1] or "").strip() or f"id {user_id}"
        profession = (worker[3] or "").strip()
        return f"👷 {full_name}" + (f" ({profession})" if profession else "")
    client = get_client(user_id)
    if client:
        contact = (client[2] or "").strip()
        company = (client[1] or "").strip()
        label = contact or company or f"id {user_id}"
        return f"🏢 {label}"
    return "Участник"


def _can_access_shift_chat(user_id: int, shift_id: int) -> bool:
    if is_admin_user(int(user_id)):
        return True
    if get_client(user_id) and client_owns_shift(user_id, shift_id):
        return True
    if get_worker(user_id) and get_assignment(shift_id, user_id):
        return True
    return False


def _can_access_project_chat(user_id: int, project_id: int) -> bool:
    if is_admin_user(int(user_id)):
        return True
    if get_client(user_id) and client_owns_project(user_id, project_id):
        return True
    if get_worker(user_id) and worker_assigned_to_project(user_id, project_id):
        return True
    return False


@router.callback_query(F.data.startswith("proj_chat_"))
async def open_project_chat(callback: types.CallbackQuery, state: FSMContext):
    raw = (callback.data or "").replace("proj_chat_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    project_id = int(raw)
    user_id = callback.from_user.id
    if not _can_access_project_chat(user_id, project_id):
        await callback.answer("Нет доступа к чату проекта.", show_alert=True)
        return
    pr = get_project(project_id)
    if not pr:
        await callback.answer("Проект не найден.", show_alert=True)
        return
    await state.update_data(project_chat_id=project_id)
    messages = get_project_chat_messages(project_id, limit=15)
    sep = "━" * 16
    text = (
        f"💬 ЧАТ ПРОЕКТА #{project_id}\n\n"
        f"{pr[1]}\n\n{sep}\n"
    )
    if messages:
        for msg in reversed(messages):
            text += f"{msg[0]}: {msg[1]}\n"
    else:
        text += "Сообщений пока нет.\n"
    text += sep
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Написать", callback_data=f"send_proj_chat_{project_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"proj_chat_{project_id}")],
            [InlineKeyboardButton(text="🔙 К проекту", callback_data=f"project_hub_{project_id}")],
        ]
    )
    await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("send_proj_chat_"))
async def prompt_project_chat_message(callback: types.CallbackQuery, state: FSMContext):
    raw = (callback.data or "").replace("send_proj_chat_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    project_id = int(raw)
    user_id = callback.from_user.id
    if not _can_access_project_chat(user_id, project_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(project_chat_id=project_id)
    await state.set_state(ProjectChatState.waiting_for_message)
    await callback.message.answer(
        "✏️ Введите сообщение для чата проекта:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
    await callback.answer()


@router.message(ProjectChatState.waiting_for_message)
async def send_project_chat_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_id = data.get("project_chat_id")
    user_id = message.from_user.id
    if project_id is None or not _can_access_project_chat(user_id, int(project_id)):
        await message.answer("❌ Нет доступа или сессия сброшена.")
        await state.clear()
        return
    body = (message.text or "").strip()
    if not body:
        await message.answer("❌ Пустое сообщение нельзя отправить.")
        return
    display_name = get_user_display_name(user_id)
    save_project_chat_message(int(project_id), user_id, display_name, body)
    await message.answer("✅ Сообщение отправлено в чат проекта.")
    await state.clear()


@router.callback_query(F.data.startswith("chat_"))
async def open_chat(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("chat_", ""))
    user_id = callback.from_user.id
    shift = get_shift(shift_id)
    if not shift:
        await safe_edit_or_resend(callback, "❌ Смена не найдена.")
        await callback.answer()
        return

    if not _can_access_shift_chat(user_id, shift_id):
        await callback.answer("Нет доступа к чату этой смены.", show_alert=True)
        return

    await state.update_data(current_chat_shift=shift_id)
    messages = get_chat_messages(shift_id, limit=15)
    sep = "━" * 16
    text = (
        f"💬 ЧАТ СМЕНЫ #{shift_id}\n\n"
        f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n📍 {shift[5]}\n\n{sep}\n"
    )
    if messages:
        for msg in reversed(messages):
            text += f"{msg[0]}: {msg[1]}\n"
    else:
        text += "Сообщений пока нет.\n"
    text += sep
    is_admin = is_admin_user(int(user_id))
    if is_admin:
        text += "\nВы вошли как координатор — можете писать в этот чат (кнопка «Написать»)."
    is_client = get_client(user_id) is not None
    if is_admin:
        back_callback = f"shift_hub_ad_{shift_id}"
    elif is_client:
        back_callback = f"shift_detail_{shift_id}"
    else:
        back_callback = f"worker_shift_{shift_id}"

    nav_row = []
    if is_admin:
        nav_row = [
            InlineKeyboardButton(text="🎯 Сводка смены", callback_data=f"shift_hub_ad_{shift_id}"),
            InlineKeyboardButton(text="🗓 Управление сменами", callback_data="admin_shift_manage"),
        ]
    else:
        nav_row = [
            InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts"),
            InlineKeyboardButton(
                text="✅ Мои задачи",
                callback_data="my_client_tasks" if is_client else "my_tasks",
            ),
        ]

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Написать", callback_data=f"send_chat_msg_{shift_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chat_{shift_id}")],
            nav_row,
            [InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)],
        ]
    )
    await safe_edit_or_resend(callback, text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("send_chat_msg_"))
async def prompt_chat_message(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("send_chat_msg_", ""))
    user_id = callback.from_user.id
    if not _can_access_shift_chat(user_id, shift_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(current_chat_shift=shift_id)
    await state.set_state(ChatMessageState.waiting_for_message)
    await callback.message.answer(
        "✏️ Напишите сообщение для чата одним текстом (можно вставить ссылку). Стикеры и фото сюда не сохраняются.",
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
    await callback.answer()


@router.message(ChatMessageState.waiting_for_message)
async def send_chat_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("current_chat_shift")
    user_id = message.from_user.id
    if shift_id is None or not _can_access_shift_chat(user_id, int(shift_id)):
        await message.answer("❌ Нет доступа или сессия сброшена.")
        await state.clear()
        return
    display_name = get_user_display_name(user_id)
    body = (message.text or message.caption or "").strip()
    if not body:
        await message.answer("Нужен текст сообщения (не стикер). Напишите ещё раз.")
        return
    save_chat_message(shift_id, user_id, display_name, body)
    await _broadcast_shift_chat_message(
        message.bot,
        shift_id=int(shift_id),
        sender_id=int(user_id),
        display_name=display_name,
        body=body,
    )
    await message.answer("✅ Сообщение отправлено!")
    await state.clear()
