# keyboards/menus.py
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

def main_menu_keyboard(is_client: bool = False, is_worker: bool = False):
    buttons = []
    
    if is_client:
        buttons.append([InlineKeyboardButton(text="📊 Обзор", callback_data="client_menu_overview")])
        buttons.append([InlineKeyboardButton(text="🗓 Смены и статусы", callback_data="client_menu_shifts")])
        buttons.append([InlineKeyboardButton(text="📋 Задачи", callback_data="client_menu_tasks")])
        buttons.append([InlineKeyboardButton(text="💬 Коммуникации", callback_data="client_menu_comms")])
    elif is_worker:
        buttons.append([InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts")])
        buttons.append([InlineKeyboardButton(text="📋 Мои задачи", callback_data="my_tasks")])
    else:
        buttons.append([InlineKeyboardButton(text="👷 Я ИСПОЛНИТЕЛЬ", callback_data="register_worker")])
        buttons.append([InlineKeyboardButton(text="🏢 Я ЗАКАЗЧИК", callback_data="register_client")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def back_to_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
    ])

def professions_keyboard():
    professions = ["Хелпер", "Грузчик", "Промоутер", "Хостес", "Гардеробщик", "Парковщик"]
    buttons = []
    row = []
    for i, prof in enumerate(professions):
        row.append(InlineKeyboardButton(text=prof, callback_data=f"prof_{prof}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def confirm_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_yes")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="confirm_edit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")]
    ])

def shift_actions_keyboard(shift_id: int, is_client: bool = False):
    buttons = []
    if is_client:
        buttons.append([InlineKeyboardButton(text="👥 Назначить исполнителей", callback_data=f"assign_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="📋 Поставить задачу", callback_data=f"add_task_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="📊 Отчёт по смене", callback_data=f"report_{shift_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Подтвердить выход", callback_data=f"confirm_shift_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="✅ Чек-ин", callback_data=f"checkin_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="💬 Чат смены", callback_data=f"chat_{shift_id}")])
        buttons.append([InlineKeyboardButton(text="📋 Мои задачи", callback_data=f"tasks_{shift_id}")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_shifts")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def client_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Мои проекты", callback_data="my_projects")],
            [InlineKeyboardButton(text="➕ Создать проект", callback_data="create_project")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
        ]
    )


def client_shifts_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts")],
            [InlineKeyboardButton(text="📡 Статус выхода на смену", callback_data="client_shift_statuses")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
        ]
    )


def client_tasks_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Мои задачи", callback_data="my_client_tasks")],
            [InlineKeyboardButton(text="📝 Поставить задачу", callback_data="client_add_task_pick_shift")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
        ]
    )


def client_comms_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Чаты смен", callback_data="client_shift_chats")],
            [InlineKeyboardButton(text="📡 Статус выхода на смену", callback_data="client_shift_statuses")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
        ]
    )
