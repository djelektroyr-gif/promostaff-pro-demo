# handlers/common.py
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import get_worker, get_client
from keyboards.menus import main_menu_keyboard
from states import WorkerRegistration, ClientRegistration

router = Router()


def _start_payload(text: str) -> str:
    parts = (text or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip().lower()


async def _send_group_onboarding(message: types.Message) -> None:
    me = await message.bot.get_me()
    username = (me.username or "").strip()
    if username:
        worker_link = f"https://t.me/{username}?start=register_worker"
        client_link = f"https://t.me/{username}?start=register_client"
    else:
        # fallback, если username не задан
        worker_link = "https://t.me/"
        client_link = "https://t.me/"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👷 Я исполнитель", url=worker_link)],
            [InlineKeyboardButton(text="🏢 Я заказчик", url=client_link)],
        ]
    )
    await message.answer(
        "👋 Добрый день! Я помощник *PROMOSTAFF* для тестового проекта.\n\n"
        "Чем могу помочь:\n"
        "• регистрация заказчика и исполнителей;\n"
        "• доступ к сменам, задачам, чату и отчётам;\n"
        "• быстрый вход в нужный сценарий по кнопкам ниже.",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


@router.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    # В группах /start не запускает личный сценарий.
    if message.chat.type in ("group", "supergroup"):
        await message.answer(
            "Для регистрации откройте бота в личных сообщениях по кнопкам из приветствия."
        )
        return

    user_id = message.from_user.id
    payload = _start_payload(message.text or "")

    # Проверяем, зарегистрирован ли пользователь
    worker = get_worker(user_id)
    client = get_client(user_id)

    if not worker and not client and payload == "register_worker":
        await state.clear()
        await message.answer(
            "👷 *Регистрация исполнителя*\n\n"
            "Шаг 1/3: Введите ваше полное ФИО:",
            parse_mode="Markdown",
        )
        await state.set_state(WorkerRegistration.full_name)
        return
    if not worker and not client and payload == "register_client":
        await state.clear()
        await message.answer(
            "🏢 *Регистрация заказчика*\n\n"
            "Шаг 1/3: Введите название компании:",
            parse_mode="Markdown",
        )
        await state.set_state(ClientRegistration.company_name)
        return

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


@router.message(F.new_chat_members)
async def group_bot_added(message: types.Message):
    # Приветствие только когда в группу добавили именно этого бота.
    me = await message.bot.get_me()
    added_ids = {u.id for u in (message.new_chat_members or [])}
    if me.id not in added_ids:
        return
    await _send_group_onboarding(message)


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
