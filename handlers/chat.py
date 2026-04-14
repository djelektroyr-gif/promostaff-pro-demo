# handlers/chat.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import (
    save_chat_message,
    get_chat_messages,
    get_shift,
    get_worker,
    get_client,
    get_assignment,
    client_owns_shift,
)
from states import ChatMessageState

router = Router()


def get_user_display_name(user_id: int) -> str:
    worker = get_worker(user_id)
    if worker:
        return f"Исполнитель ({worker[3]})"
    client = get_client(user_id)
    if client:
        return "Заказчик"
    return "Участник"


def _can_access_shift_chat(user_id: int, shift_id: int) -> bool:
    if get_client(user_id) and client_owns_shift(user_id, shift_id):
        return True
    if get_worker(user_id) and get_assignment(shift_id, user_id):
        return True
    return False


@router.callback_query(F.data.startswith("chat_"))
async def open_chat(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("chat_", ""))
    user_id = callback.from_user.id
    shift = get_shift(shift_id)
    if not shift:
        await callback.message.edit_text("❌ Смена не найдена.")
        await callback.answer()
        return

    if not _can_access_shift_chat(user_id, shift_id):
        await callback.answer("Нет доступа к чату этой смены.", show_alert=True)
        return

    await state.update_data(current_chat_shift=shift_id)
    messages = get_chat_messages(shift_id, limit=15)

    text = (
        f"💬 *ЧАТ СМЕНЫ #{shift_id}*\n\n"
        f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n📍 {shift[5]}\n\n━━━━━━━━━━━━━━━━\n"
    )
    if messages:
        for msg in reversed(messages):
            text += f"*{msg[0]}*: {msg[1]}\n"
    else:
        text += "_Сообщений пока нет._\n"
    text += "━━━━━━━━━━━━━━━━"

    is_client = get_client(user_id) is not None
    back_callback = f"shift_detail_{shift_id}" if is_client else f"worker_shift_{shift_id}"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Написать", callback_data=f"send_chat_msg_{shift_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)],
        ]
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
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
    await callback.message.answer("✏️ Введите ваше сообщение:")
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
    save_chat_message(shift_id, user_id, display_name, message.text or "")
    await message.answer("✅ Сообщение отправлено!")
    await state.clear()
