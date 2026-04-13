# handlers/tasks.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import (
    create_task, get_worker_tasks, complete_task, get_shift_tasks,
    get_shift, get_worker, get_client, get_shift_assignments
)
from states import TaskCreation, TaskCompletion

router = Router()

# ========== СОЗДАНИЕ ЗАДАЧИ (ЗАКАЗЧИК) ==========
@router.callback_query(F.data.startswith("add_task_"))
async def add_task_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("add_task_", ""))
    await state.update_data(task_shift_id=shift_id)
    
    await callback.message.edit_text(
        "📋 *НОВАЯ ЗАДАЧА*\n\n"
        "Введите название задачи:",
        parse_mode="Markdown"
    )
    await state.set_state(TaskCreation.title)
    await callback.answer()

@router.message(TaskCreation.title)
async def task_title_received(message: types.Message, state: FSMContext):
    await state.update_data(task_title=message.text.strip())
    await message.answer("Введите описание задачи (или отправьте `-` если не нужно):")
    await state.set_state(TaskCreation.description)

@router.message(TaskCreation.description)
async def task_description_received(message: types.Message, state: FSMContext):
    description = message.text.strip()
    if description == "-":
        description = ""
    await state.update_data(task_description=description)
    
    data = await state.get_data()
    shift_id = data['task_shift_id']
    
    # Получаем список исполнителей на смене
    assignments = get_shift_assignments(shift_id)
    
    if not assignments:
        await message.answer("❌ На эту смену ещё не назначены исполнители.")
        await state.clear()
        return
    
    keyboard_rows = []
    for a in assignments:
        keyboard_rows.append([InlineKeyboardButton(
            text=a[5],  # full_name
            callback_data=f"assign_task_{a[1]}"  # worker_id
        )])
    keyboard_rows.append([InlineKeyboardButton(text="❌ Пропустить", callback_data="assign_task_skip")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    
    await message.answer(
        "👤 Выберите исполнителя для этой задачи:",
        reply_markup=keyboard
    )
    await state.set_state(TaskCreation.assigned_to)

@router.callback_query(TaskCreation.assigned_to, F.data.startswith("assign_task_"))
async def task_assign_received(callback: types.CallbackQuery, state: FSMContext):
    worker_id = None if callback.data == "assign_task_skip" else int(callback.data.replace("assign_task_", ""))
    
    data = await state.get_data()
    shift_id = data['task_shift_id']
    title = data['task_title']
    description = data.get('task_description', '')
    
    task_id = create_task(shift_id, title, description, worker_id)
    
    await callback.message.edit_text(
        f"✅ Задача создана!\n\n"
        f"📋 {title}\n"
        f"👤 Назначена: {'исполнителю' if worker_id else 'без назначения'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 К смене", callback_data=f"shift_detail_{shift_id}")]
        ])
    )
    
    # Уведомляем исполнителя
    if worker_id:
        shift = get_shift(shift_id)
        await callback.bot.send_message(
            worker_id,
            f"📋 *НОВАЯ ЗАДАЧА*\n\n"
            f"Смена #{shift_id} ({shift[2]})\n\n"
            f"*{title}*\n"
            f"{description}\n\n"
            f"Выполните задачу и отправьте отчёт.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Отчитаться", callback_data=f"complete_task_{task_id}")]
            ])
        )
    
    await state.clear()
    await callback.answer()

# ========== ПРОСМОТР ЗАДАЧ (ИСПОЛНИТЕЛЬ) ==========
@router.callback_query(F.data.startswith("tasks_"))
async def show_my_tasks_for_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("tasks_", ""))
    user_id = callback.from_user.id
    
    tasks = get_worker_tasks(shift_id, user_id)
    
    if not tasks:
        await callback.message.edit_text(
            "📋 У вас нет задач на этой смене.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"worker_shift_{shift_id}")]
            ])
        )
        await callback.answer()
        return
    
    text = f"📋 *ВАШИ ЗАДАЧИ НА СМЕНУ #{shift_id}*\n\n"
    keyboard_rows = []
    
    for t in tasks:
        status_emoji = "✅" if t[6] == 'completed' else "⏳"
        text += f"{status_emoji} *{t[2]}*\n"
        if t[3]:
            text += f"  _{t[3]}_\n"
        text += "\n"
        
        if t[6] != 'completed':
            keyboard_rows.append([InlineKeyboardButton(
                text=f"📝 Отчитаться: {t[2][:20]}...",
                callback_data=f"complete_task_{t[0]}"
            )])
    
    keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"worker_shift_{shift_id}")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

# ========== ВЫПОЛНЕНИЕ ЗАДАЧИ ==========
@router.callback_query(F.data.startswith("complete_task_"))
async def complete_task_start(callback: types.CallbackQuery, state: FSMContext):
    task_id = int(callback.data.replace("complete_task_", ""))
    await state.update_data(completing_task_id=task_id)
    
    await callback.message.edit_text(
        "📝 *ОТЧЁТ ПО ЗАДАЧЕ*\n\n"
        "Опишите, что было сделано (или отправьте `-` чтобы пропустить):",
        parse_mode="Markdown"
    )
    await state.set_state(TaskCompletion.report_text)
    await callback.answer()

@router.message(TaskCompletion.report_text)
async def task_report_text_received(message: types.Message, state: FSMContext):
    report_text = message.text.strip()
    if report_text == "-":
        report_text = ""
    
    await state.update_data(report_text=report_text)
    
    await message.answer(
        "📸 Отправьте фотоотчёт (или `0` чтобы пропустить):"
    )
    await state.set_state(TaskCompletion.report_photo)

@router.message(TaskCompletion.report_photo, F.photo)
async def task_report_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await finish_task_completion(message, state, photo_id)

@router.message(TaskCompletion.report_photo, F.text)
async def task_report_skip_photo(message: types.Message, state: FSMContext):
    if message.text == "0":
        await finish_task_completion(message, state, None)
    else:
        await message.answer("Отправьте фото или `0` чтобы пропустить.")

async def finish_task_completion(message: types.Message, state: FSMContext, photo_id: str = None):
    data = await state.get_data()
    task_id = data['completing_task_id']
    report_text = data.get('report_text', '')
    
    complete_task(task_id, report_text, photo_id)
    
    await message.answer(
        "✅ Задача отмечена как выполненная!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
    )
    
    await state.clear()
