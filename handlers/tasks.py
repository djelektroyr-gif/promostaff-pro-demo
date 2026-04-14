# handlers/tasks.py
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db import (
    create_task,
    get_worker_tasks,
    complete_task,
    get_shift,
    get_shift_assignments,
    list_shifts_with_open_tasks_for_worker,
    get_worker,
)
from states import TaskCreation, TaskCompletion

router = Router()

T_STATUS = 5


@router.callback_query(F.data == "my_tasks")
async def my_tasks_hub(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_worker(user_id):
        await callback.answer("Только для исполнителя.", show_alert=True)
        return
    shifts = list_shifts_with_open_tasks_for_worker(user_id)
    if not shifts:
        await callback.message.edit_text(
            "📋 Нет открытых задач. Если задачи назначены — выберите смену в «Мои смены».",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    text = "📋 *Смены с незавершёнными задачами:*\n\n"
    for s in shifts:
        text += f"• {s[1]} {s[2]}-{s[3]}\n"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{s[1]} → задачи",
                    callback_data=f"tasks_{s[0]}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("add_task_"))
async def add_task_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("add_task_", ""))
    await state.update_data(task_shift_id=shift_id)
    await callback.message.edit_text("📋 *НОВАЯ ЗАДАЧА*\n\nВведите название задачи:", parse_mode="Markdown")
    await state.set_state(TaskCreation.title)
    await callback.answer()


@router.message(TaskCreation.title)
async def task_title_received(message: types.Message, state: FSMContext):
    await state.update_data(task_title=message.text.strip())
    await message.answer("Введите описание задачи (или `-`):")
    await state.set_state(TaskCreation.description)


@router.message(TaskCreation.description)
async def task_description_received(message: types.Message, state: FSMContext):
    description = message.text.strip()
    if description == "-":
        description = ""
    await state.update_data(task_description=description)

    data = await state.get_data()
    shift_id = data["task_shift_id"]
    assignments = get_shift_assignments(shift_id)

    if not assignments:
        await message.answer("❌ На эту смену ещё не назначены исполнители.")
        await state.clear()
        return

    keyboard_rows = []
    for a in assignments:
        wid = a[2]
        name = a[11] if len(a) > 11 else f"id{wid}"
        keyboard_rows.append(
            [InlineKeyboardButton(text=name[:40], callback_data=f"assign_task_{wid}")]
        )
    keyboard_rows.append([InlineKeyboardButton(text="❌ Без исполнителя", callback_data="assign_task_skip")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

    await message.answer("👤 Выберите исполнителя:", reply_markup=keyboard)
    await state.set_state(TaskCreation.assigned_to)


@router.callback_query(TaskCreation.assigned_to, F.data.startswith("assign_task_"))
async def task_assign_received(callback: types.CallbackQuery, state: FSMContext):
    worker_id = None if callback.data == "assign_task_skip" else int(callback.data.replace("assign_task_", ""))
    data = await state.get_data()
    shift_id = data["task_shift_id"]
    title = data["task_title"]
    description = data.get("task_description", "")

    task_id = create_task(shift_id, title, description, worker_id)

    await callback.message.edit_text(
        f"✅ Задача создана!\n\n📋 {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )

    if worker_id:
        shift = get_shift(shift_id)
        await callback.bot.send_message(
            worker_id,
            f"📋 *НОВАЯ ЗАДАЧА*\n\nСмена #{shift_id} ({shift[2]})\n\n*{title}*\n{description}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Отчитаться", callback_data=f"complete_task_{task_id}")]
                ]
            ),
        )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data.startswith("tasks_"))
async def show_my_tasks_for_shift(callback: types.CallbackQuery):
    shift_id = int(callback.data.replace("tasks_", ""))
    user_id = callback.from_user.id
    tasks = get_worker_tasks(shift_id, user_id)

    if not tasks:
        await callback.message.edit_text(
            "📋 У вас нет задач на этой смене.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=f"worker_shift_{shift_id}")]
                ]
            ),
        )
        await callback.answer()
        return

    text = "📋 *ВАШИ ЗАДАЧИ*\n\n"
    keyboard_rows = []
    for t in tasks:
        status_emoji = "✅" if t[T_STATUS] == "completed" else "⏳"
        text += f"{status_emoji} *{t[2]}*\n"
        if t[T_STATUS] != "completed":
            title_short = (t[2] or "")[:20]
            keyboard_rows.append(
                [InlineKeyboardButton(text=f"📝 {title_short}", callback_data=f"complete_task_{t[0]}")]
            )

    keyboard_rows.append(
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"worker_shift_{shift_id}")]
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("complete_task_"))
async def complete_task_start(callback: types.CallbackQuery, state: FSMContext):
    task_id = int(callback.data.replace("complete_task_", ""))
    await state.update_data(completing_task_id=task_id)
    await callback.message.edit_text(
        "📝 *ОТЧЁТ*\n\nОпишите, что сделано (или `-`):",
        parse_mode="Markdown",
    )
    await state.set_state(TaskCompletion.report_text)
    await callback.answer()


@router.message(TaskCompletion.report_text)
async def task_report_text_received(message: types.Message, state: FSMContext):
    report_text = message.text.strip()
    if report_text == "-":
        report_text = ""
    await state.update_data(report_text=report_text)
    await message.answer("📸 Отправьте фото (или `0`):")
    await state.set_state(TaskCompletion.report_photo)


@router.message(TaskCompletion.report_photo, F.photo)
async def task_report_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    task_id = data["completing_task_id"]
    report_text = data.get("report_text", "")
    complete_task(task_id, report_text, photo_id)
    await message.answer("✅ Задача выполнена!")
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)
async def task_report_skip_photo(message: types.Message, state: FSMContext):
    if message.text == "0":
        data = await state.get_data()
        task_id = data["completing_task_id"]
        report_text = data.get("report_text", "")
        complete_task(task_id, report_text, None)
        await message.answer("✅ Задача выполнена!")
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")
