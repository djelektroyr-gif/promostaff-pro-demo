# handlers/common.py
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardRemove

from db import get_worker, get_client
from keyboards.menus import main_menu_keyboard

router = Router()

@router.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    worker = get_worker(user_id)
    client = get_client(user_id)
    
    if worker:
        await message.answer(
            f"👷 *Добро пожаловать, {worker[1]}!*\n\n"
            "Вы зарегистрированы как исполнитель.\n"
            "Выберите действие:",
            reply_markup=main_menu_keyboard(is_worker=True),
            parse_mode="Markdown"
        )
    elif client:
        await message.answer(
            f"🏢 *Добро пожаловать, {client[2]}!*\n\n"
            f"Компания: {client[1]}\n"
            "Выберите действие:",
            reply_markup=main_menu_keyboard(is_client=True),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "🌟 *Добро пожаловать в PROMOSTAFF DEMO!*\n\n"
            "Выберите вашу роль:",
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

@router.callback_query(F.data == "main_menu")
async def show_main_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    worker = get_worker(user_id)
    client = get_client(user_id)
    
    if worker:
        text = f"👷 *{worker[1]}*\n\nВыберите действие:"
        markup = main_menu_keyboard(is_worker=True)
    elif client:
        text = f"🏢 *{client[2]}* ({client[1]})\n\nВыберите действие:"
        markup = main_menu_keyboard(is_client=True)
    else:
        text = "🌟 *Добро пожаловать!*\n\nВыберите вашу роль:"
        markup = main_menu_keyboard()
    
    await callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    await callback.answer()
