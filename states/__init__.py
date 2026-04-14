# states/__init__.py
from aiogram.fsm.state import State, StatesGroup


class WorkerRegistration(StatesGroup):
    full_name = State()
    phone = State()
    profession = State()
    confirm = State()


class ClientRegistration(StatesGroup):
    company_name = State()
    contact_name = State()
    phone = State()


class ProjectCreation(StatesGroup):
    """Сначала выбор заказчика (callback), затем ввод названия."""
    enter_name = State()


class ShiftAdminLine(StatesGroup):
    """Одна строка: ДД.ММ.ГГГГ | ЧЧ:ММ | ЧЧ:ММ | адрес | ставка"""
    line = State()


class ShiftCreation(StatesGroup):
    date = State()
    start_time = State()
    end_time = State()
    location = State()
    coords = State()
    radius = State()
    rate = State()


class TaskCreation(StatesGroup):
    shift_id = State()
    title = State()
    description = State()
    assigned_to = State()


class TaskCompletion(StatesGroup):
    task_id = State()
    report_text = State()
    report_photo = State()


class ChatMessageState(StatesGroup):
    shift_id = State()
    waiting_for_message = State()


class CheckinFlow(StatesGroup):
    geo = State()
    photo = State()


class CheckoutFlow(StatesGroup):
    geo = State()
    photo = State()
