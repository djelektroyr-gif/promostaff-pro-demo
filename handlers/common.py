# handlers/common.py
from __future__ import annotations

import logging

from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import PARSE_MODE_TELEGRAM, is_admin_user
from db import get_worker, get_client
from keyboards.menus import (
    main_menu_keyboard,
    client_overview_keyboard,
    client_shifts_keyboard,
    client_tasks_keyboard,
    client_comms_keyboard,
)
from services.text_utils import bold, escape_markdown as em
from states import WorkerRegistration, ClientRegistration

from .telegram_edit import safe_edit_or_resend

logger = logging.getLogger(__name__)

router = Router()
# Регистрируется последним в handlers/__init__.py — не перехватывает шаги FSM других роутеров.
fallback_router = Router()


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
        worker_link = "https://t.me/"
        client_link = "https://t.me/"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👷 Я исполнитель", url=worker_link)],
            [InlineKeyboardButton(text="🏢 Я заказчик", url=client_link)],
        ]
    )
    body = (
        em("👋 Добрый день! Я помощник PROMOSTAFF для тестового проекта.")
        + "\n\n"
        + em("Чем могу помочь:")
        + "\n"
        + "• "
        + em("регистрация заказчика и исполнителей;")
        + "\n"
        + "• "
        + em("доступ к сменам, задачам, чату и отчётам;")
        + "\n"
        + "• "
        + em("быстрый вход в нужный сценарий по кнопкам ниже.")
        + "\n\n"
        + em(
            "Если приветствие пропало: в этом чате можно написать команду /start или /promostaff "
            "(с косой чертой в начале, как обычная команда в Telegram)."
        )
    )
    await message.answer(body, parse_mode=PARSE_MODE_TELEGRAM, reply_markup=keyboard)


@router.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    if message.chat.type in ("group", "supergroup"):
        await _send_group_onboarding(message)
        return

    user_id = message.from_user.id
    payload = _start_payload(message.text or "")
    admin = is_admin_user(user_id)

    worker = get_worker(user_id)
    client = get_client(user_id)

    if not worker and not client and payload == "register_worker":
        await state.clear()
        await message.answer(
            bold("Регистрация исполнителя") + "\n\n" + em("Шаг 1/3: Введите ваше полное ФИО:"),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        await state.set_state(WorkerRegistration.full_name)
        return
    if not worker and not client and payload == "register_client":
        await state.clear()
        await message.answer(
            bold("Регистрация заказчика") + "\n\n" + em("Шаг 1/3: Введите название компании:"),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        await state.set_state(ClientRegistration.company_name)
        return

    if admin:
        if not worker and not client:
            title = bold("Администратор") + "\n\n"
        else:
            role = "исполнитель" if worker else "заказчик" if client else "гость"
            title = bold(f"Администратор / {role}") + "\n\n"
        await message.answer(
            title + em("Выберите действие:"),
            reply_markup=main_menu_keyboard(
                is_client=bool(client),
                is_worker=bool(worker),
                is_admin=True,
            ),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
    elif worker:
        await message.answer(
            "👷 "
            + bold(f"Добро пожаловать, {worker[1]}!")
            + "\n\n"
            + em("Вы зарегистрированы как исполнитель.")
            + "\n"
            + em("Выберите действие:"),
            reply_markup=main_menu_keyboard(is_worker=True),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
    elif client:
        await message.answer(
            "🏢 "
            + bold(f"Добро пожаловать, {client[2]}!")
            + "\n\n"
            + em(f"Компания: {client[1]}")
            + "\n"
            + em("Выберите действие:"),
            reply_markup=main_menu_keyboard(is_client=True),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
    else:
        await message.answer(
            "🌟 "
            + bold("Добро пожаловать в PROMOSTAFF DEMO!")
            + "\n\n"
            + em("Выберите вашу роль:"),
            reply_markup=main_menu_keyboard(),
            parse_mode=PARSE_MODE_TELEGRAM,
        )


@router.message(F.new_chat_members)
async def group_bot_added(message: types.Message):
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
    admin = is_admin_user(user_id)

    if admin:
        text = bold("Главное меню") + "\n\n" + em("Выберите действие:")
        markup = main_menu_keyboard(
            is_client=bool(client), is_worker=bool(worker), is_admin=True
        )
    elif worker:
        text = "👷 " + bold(str(worker[1])) + "\n\n" + em("Выберите действие:")
        markup = main_menu_keyboard(is_worker=True)
    elif client:
        text = (
            "🏢 "
            + bold(str(client[2]))
            + em(f" ({client[1]})")
            + "\n\n"
            + em("Выберите действие:")
        )
        markup = main_menu_keyboard(is_client=True)
    else:
        text = "🌟 " + bold("Добро пожаловать!") + "\n\n" + em("Выберите вашу роль:")
        markup = main_menu_keyboard()

    await safe_edit_or_resend(callback, text, reply_markup=markup, parse_mode=PARSE_MODE_TELEGRAM)
    await callback.answer()


@router.callback_query(StateFilter("*"), F.data == "cancel_flow")
async def cancel_any_flow(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await show_main_menu(callback)


@router.message(StateFilter("*"), Command("cancel"))
async def cancel_any_flow_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "✅ Действие отменено.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🏠 В главное меню", callback_data="main_menu")]]
        ),
    )


@router.callback_query(F.data == "client_menu_overview")
async def client_menu_overview(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await safe_edit_or_resend(
        callback,
        bold("Обзор заказчика")
        + "\n\n"
        + em("Здесь — ваши проекты. Смены создаёт администратор; вы смотрите людей, ставите задачи и читаете отчёты.")
        + "\n\n"
        + em("Дальше нажмите нужную кнопку внизу."),
        parse_mode=PARSE_MODE_TELEGRAM,
        reply_markup=client_overview_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_shifts")
async def client_menu_shifts(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await safe_edit_or_resend(
        callback,
        bold("Смены")
        + "\n\n"
        + "• "
        + em("Мои смены — карточки по датам.")
        + "\n"
        + "• "
        + em("Кто подтвердил выход — сводка по всем сменам.")
        + "\n\n"
        + em("Чек-ины и чек-ауты исполнители делают сами в боте."),
        parse_mode=PARSE_MODE_TELEGRAM,
        reply_markup=client_shifts_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_tasks")
async def client_menu_tasks(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await safe_edit_or_resend(
        callback,
        bold("Задачи для исполнителей")
        + "\n\n"
        + em("1) ")
        + em('Нажмите «Новая задача для исполнителя».')
        + "\n"
        + em("2) ")
        + em("Выберите смену → введите название и описание.")
        + "\n"
        + em("3) ")
        + em("Укажите, кому на смене отправить — люди получат уведомление.")
        + "\n\n"
        + em('«Смотреть все задачи» — список и оценки после выполнения.'),
        parse_mode=PARSE_MODE_TELEGRAM,
        reply_markup=client_tasks_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "client_menu_comms")
async def client_menu_comms(callback: types.CallbackQuery):
    if not get_client(callback.from_user.id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    await safe_edit_or_resend(
        callback,
        bold("Связь")
        + "\n\n"
        + "• "
        + em("Чаты смен — общий чат по каждой смене.")
        + "\n"
        + "• "
        + em("Кто подтвердил выход — если нужно уточнить состав.")
        + "\n\n"
        + em('Писать исполнителю лично можно из карточки смены (кнопка «Написать»).'),
        parse_mode=PARSE_MODE_TELEGRAM,
        reply_markup=client_comms_keyboard(),
    )
    await callback.answer()


@fallback_router.message(F.chat.type == "private")
async def fsm_or_start_hint(message: types.Message, state: FSMContext):
    st = await state.get_state()
    if st:
        await message.answer(
            em("Бот ждёт ввод по текущему шагу. Завершите шаг, нажмите «❌ Отмена» или отправьте /cancel."),
            parse_mode=PARSE_MODE_TELEGRAM,
        )
        return
    await message.answer(
        em("Команда не распознана. Отправьте /start, чтобы открыть главное меню."),
        parse_mode=PARSE_MODE_TELEGRAM,
    )
