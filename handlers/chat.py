# handlers/chat.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import save_chat_message, get_chat_messages, get_shift, get_worker, get_client
from states import ChatMessageState

router = Router()

def get_user_display_name(user_id: int) -> str:
    """Возвращает обезличенное имя для чата."""
    worker = get_worker(user_id)
    if worker:
        # Исполнитель: "Исполнитель (Профессия)"
        return f"Исполнитель ({worker[3]})"
    
    client = get_client(user_id)
    if client:
        # Заказчик: "Заказчик"
        return "Заказчик"
    
    return "Участник"

@router.callback_query(F.data.startswith("chat_"))
async def open_chat(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("chat_", ""))
    user_id = callback.from_user.id
    
    # Проверяем, имеет ли пользователь доступ к этой смене
    shift = get_shift(shift_id)
    if not shift:
        await callback.message.edit_text("❌ Смена не найдена.")
        await callback.answer()
        return
    
    await state.update_data(current_chat_shift=shift_id)
    
    messages = get_chat_messages(shift_id, limit=15)
    
    text = f"💬 *ЧАТ СМЕНЫ #{shift_id}*\n\n"
    text += f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n"
    text += f"📍 {shift[5]}\n\n"
    text += "━━━━━━━━━━━━━━━━\n"
    
    if messages:
        for msg in reversed(messages):
            text += f"*{msg[0]}*: {msg[1]}\n"
    else:
        text += "_Сообщений пока нет. Начните общение!_\n"
    
    text += "━━━━━━━━━━━━━━━━"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Написать", callback_data=f"send_chat_msg_{shift_id}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chat_{shift_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"shift_detail_{shift_id}" if get_client(user_id) else f"worker_shift_{shift_id}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data.startswith("send_chat_msg_"))
async def prompt_chat_message(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("send_chat_msg_", ""))
    await state.update_data(current_chat_shift=shift_id)
    await state.set_state(ChatMessageState.waiting_for_message)
    
    await callback.message.answer(
        "✏️ Введите ваше сообщение (или отправьте /cancel для отмены):"
    )
    await callback.answer()

@router.message(F.text, ChatMessageState.waiting_for_message)
async def send_chat_message(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отправка отменена.")
        return
    
    data = await state.get_data()
    shift_id = data.get("current_chat_shift")
    user_id = message.from_user.id
    
    # Получаем обезличенное имя
    display_name = get_user_display_name(user_id)
    
    # Сохраняем сообщение
    save_chat_message(shift_id, user_id, display_name, message.text)
    
    await message.answer("✅ Сообщение отправлено!")
    
    # Возвращаемся в чат
    shift = get_shift(shift_id)
    if shift:
        messages = get_chat_messages(shift_id, limit=15)
        
        text = f"💬 *ЧАТ СМЕНЫ #{shift_id}*\n\n"
        text += f"📆 {shift[2]} | {shift[3]}-{shift[4]}\n"
        text += f"📍 {shift[5]}\n\n"
        text += "━━━━━━━━━━━━━━━━\n"
        
        for msg in reversed(messages):
            text += f"*{msg[0]}*: {msg[1]}\n"
        
        text += "━━━━━━━━━━━━━━━━"
        
        is_client = get_client(user_id) is not None
        back_callback = f"shift_detail_{shift_id}" if is_client else f"worker_shift_{shift_id}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Написать", callback_data=f"send_chat_msg_{shift_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chat_{shift_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)]
        ])
        
        await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")
    
    await state.clear()
