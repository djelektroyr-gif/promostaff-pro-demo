# handlers/registration.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

from db import save_worker, save_client
from keyboards.menus import main_menu_keyboard, professions_keyboard, confirm_keyboard
from states import WorkerRegistration, ClientRegistration

router = Router()

# ========== РЕГИСТРАЦИЯ ИСПОЛНИТЕЛЯ ==========
@router.callback_query(F.data == "register_worker")
async def start_worker_reg(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "👷 *Регистрация исполнителя*\n\n"
        "Шаг 1/3: Введите ваше полное ФИО:",
        parse_mode="Markdown"
    )
    await state.set_state(WorkerRegistration.full_name)
    await callback.answer()

@router.message(WorkerRegistration.full_name)
async def process_worker_name(message: types.Message, state: FSMContext):
    if len(message.text.split()) < 2:
        await message.answer("❌ Введите полное ФИО (минимум 2 слова):")
        return
    
    await state.update_data(full_name=message.text.strip())
    
    # Клавиатура для запроса телефона
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить контакт", request_contact=True)]],
        resize_keyboard=True
    )
    
    await message.answer(
        "Шаг 2/3: Отправьте ваш номер телефона.\n"
        "Нажмите кнопку ниже:",
        reply_markup=keyboard
    )
    await state.set_state(WorkerRegistration.phone)

@router.message(WorkerRegistration.phone, F.contact)
async def process_worker_phone_contact(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await message.answer(
        "Шаг 3/3: Выберите вашу профессию:",
        reply_markup=professions_keyboard()
    )
    await state.set_state(WorkerRegistration.profession)

@router.message(WorkerRegistration.phone, F.text)
async def process_worker_phone_text(message: types.Message, state: FSMContext):
    # Простая валидация телефона
    phone = message.text.strip()
    if not phone.startswith("+") and not phone.startswith("8"):
        await message.answer("❌ Введите номер в формате +7XXXXXXXXXX или нажмите кнопку:")
        return
    
    await state.update_data(phone=phone)
    await message.answer(
        "Шаг 3/3: Выберите вашу профессию:",
        reply_markup=professions_keyboard()
    )
    await state.set_state(WorkerRegistration.profession)

@router.callback_query(WorkerRegistration.profession, F.data.startswith("prof_"))
async def process_worker_profession(callback: types.CallbackQuery, state: FSMContext):
    profession = callback.data.replace("prof_", "")
    await state.update_data(profession=profession)
    
    data = await state.get_data()
    
    text = (
        "📋 *Проверьте данные:*\n\n"
        f"👤 ФИО: {data['full_name']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"💼 Профессия: {profession}\n\n"
        "Всё верно?"
    )
    
    await callback.message.edit_text(text, reply_markup=confirm_keyboard(), parse_mode="Markdown")
    await state.set_state(WorkerRegistration.confirm)
    await callback.answer()

@router.callback_query(WorkerRegistration.confirm, F.data == "confirm_yes")
async def confirm_worker_reg(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    save_worker(callback.from_user.id, data)
    
    await callback.message.edit_text(
        "✅ *Регистрация завершена!*\n\n"
        "Теперь вы можете получать смены и работать.",
        parse_mode="Markdown"
    )
    await state.clear()
    
    # Показываем главное меню
    await callback.message.answer(
        f"👷 *{data['full_name']}*\n\nВыберите действие:",
        reply_markup=main_menu_keyboard(is_worker=True),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(WorkerRegistration.confirm, F.data == "confirm_edit")
async def edit_worker_reg(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await start_worker_reg(callback, state)

# ========== РЕГИСТРАЦИЯ ЗАКАЗЧИКА (УПРОЩЁННАЯ) ==========
@router.callback_query(F.data == "register_client")
async def start_client_reg(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🏢 *Регистрация заказчика*\n\n"
        "Шаг 1/3: Введите название компании:",
        parse_mode="Markdown"
    )
    await state.set_state(ClientRegistration.company_name)
    await callback.answer()

@router.message(ClientRegistration.company_name)
async def process_company_name(message: types.Message, state: FSMContext):
    await state.update_data(company_name=message.text.strip())
    await message.answer("Шаг 2/3: Введите ваше имя (контактное лицо):")
    await state.set_state(ClientRegistration.contact_name)

@router.message(ClientRegistration.contact_name)
async def process_contact_name(message: types.Message, state: FSMContext):
    await state.update_data(contact_name=message.text.strip())
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить контакт", request_contact=True)]],
        resize_keyboard=True
    )
    
    await message.answer(
        "Шаг 3/3: Отправьте ваш номер телефона:",
        reply_markup=keyboard
    )
    await state.set_state(ClientRegistration.phone)

@router.message(ClientRegistration.phone, F.contact)
async def process_client_phone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    save_client(message.from_user.id, {
        **data,
        'phone': message.contact.phone_number
    })
    
    await message.answer(
        f"✅ *Регистрация завершена!*\n\n"
        f"🏢 {data['company_name']}\n"
        f"👤 {data['contact_name']}",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="Markdown"
    )
    await state.clear()
    
    await message.answer(
        f"🏢 *{data['contact_name']} ({data['company_name']})*\n\nВыберите действие:",
        reply_markup=main_menu_keyboard(is_client=True),
        parse_mode="Markdown"
    )
