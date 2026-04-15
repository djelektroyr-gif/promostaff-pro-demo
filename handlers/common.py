# handlers/common.py
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_ID
from db import get_worker, get_client
from keyboards.menus import (
    main_menu_keyboard,
    client_overview_keyboard,
    client_shifts_keyboard,
    client_tasks_keyboard,
    client_comms_keyboard,
)
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
        "• быстрый вход в нужный сценарий по кнопкам ниже.\n\n"
        "Если приветствие пропало: в этом чате можно написать команду /start или /promostaff "
        "(с косой чертой в начале, как обычная команда в Telegram).",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


@router.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    if message.chat.type in ("group", "supergroup"):
        await _send_group_onboarding(message)
        return

    user_id = message.from_user.id
    payload = _start_payload(message.text or "")
    is_admin = int(user_id) == int(ADMIN_USER_ID)

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

    if is_admin:
        title = (
            f"🔐 *Администратор*\n\n"
            if not worker and not client
            else f"🔐 *Администратор* / "
            f"{'исполнитель' if worker else 'заказчик' if client else 'гость'}\n\n"
        )
        await message.answer(
            title + "Выберите действие:",
            reply_markup=main_menu_keyboard(
                is_client=bool(client),
                is_worker=bool(worker),
                is_admin=True,
            ),
            parse_mode="Markdown",
        )
    elif worker:
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


@router.message(Command("promostaff"))
async def promostaff_group_cmd(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return
    await _send_group_onboarding(message)


@router.callback_query(F.data == "main_menu")
async def show_main_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    worker = get_worker(user_id)
    client = get_client(user_id)
    is_admin = int(user_id) == int(ADMIN_USER_ID)

    if is_admin:
        text = "🔐 *Главное меню*\n\nВыберите действие:"
        markup = main_menu_keyboard(
            is_client=bool(client), is_worker=bool(worker), is_admin=True
        )
    elif worker:
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


@router.callback_query(F.data == "client_menu_overview")
async def client_menu_overview(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await callback.message.edit_text(
        "📊 *Обзор заказчика*\n\n"
        "Здесь — ваши *проекты*. Смены создаёт администратор; вы смотрите людей, ставите задачи и читаете отчёты.\n\n"
        "Дальше нажмите нужную кнопку внизу.",
        parse_mode="Markdown",
        reply_markup=client_overview_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_shifts")
async def client_menu_shifts(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await callback.message.edit_text(
        "🗓 *Смены*\n\n"
        "• *Мои смены* — карточки по датам.\n"
        "• *Кто подтвердил выход* — сводка по всем сменам.\n\n"
        "Чек-ины и чек-ауты исполнители делают сами в боте.",
        parse_mode="Markdown",
        reply_markup=client_shifts_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_tasks")
async def client_menu_tasks(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await callback.message.edit_text(
        "📋 *Задачи для исполнителей*\n\n"
        "1) Нажмите *«Новая задача для исполнителя»*.\n"
        "2) Выберите смену → введите название и описание.\n"
        "3) Укажите, кому на смене отправить — люди получат уведомление.\n\n"
        "«Смотреть все задачи» — список и оценки после выполнения.",
        parse_mode="Markdown",
        reply_markup=client_tasks_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_comms")
async def client_menu_comms(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await callback.message.edit_text(
        "💬 *Связь*\n\n"
        "• *Чаты смен* — общий чат по каждой смене.\n"
        "• *Кто подтвердил выход* — если нужно уточнить состав.\n\n"
        "Писать исполнителю лично можно из карточки смены (кнопка «Написать»).",
        parse_mode="Markdown",
        reply_markup=client_comms_keyboard(),
    )
    await callback.answer()
