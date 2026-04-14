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
    get_shift_with_owner,
    get_task,
    get_client,
    list_tasks_for_client,
    list_shifts_for_client,
    assignment_join_worker_name,
    format_date_ru,
    client_owns_task,
    set_task_client_rating,
)
from states import TaskCreation, TaskCompletion

router = Router()

T_STATUS = 5


def _assignment_worker_ids(assignments) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for a in assignments:
        w = int(a[2])
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def _multi_pick_keyboard(assignments, picked: list[int]) -> InlineKeyboardMarkup:
    picked_set = set(picked)
    rows = []
    for a in assignments:
        wid = int(a[2])
        name = assignment_join_worker_name(a)
        mark = "\u2713 " if wid in picked_set else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark}{name[:35]}",
                    callback_data=f"assign_toggle_{wid}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=f"\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0434\u043b\u044f \u0432\u044b\u0431\u0440\u0430\u043d\u043d\u044b\u0445 ({len(picked)})",
                callback_data="assign_multi_done",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="\u041e\u0442\u043c\u0435\u043d\u0430",
                callback_data="assign_multi_cancel",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _notify_workers_new_tasks(
    bot,
    *,
    shift_id: int,
    shift_date_fmt: str,
    title: str,
    description: str,
    worker_task_pairs: list[tuple[int, int]],
) -> None:
    for worker_id, task_id in worker_task_pairs:
        try:
            await bot.send_message(
                worker_id,
                f"\U0001f4cb *НОВАЯ ЗАДАЧА*\n\nСмена #{shift_id} ({shift_date_fmt})\n\n*{title}*\n{description}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="\U0001f4dd Отчитаться",
                                callback_data=f"complete_task_{task_id}",
                            )
                        ]
                    ]
                ),
            )
        except Exception:
            pass


async def _notify_client_task_completed(
    bot,
    shift_row: tuple,
    task: tuple,
    report_text: str,
    photo_id: str | None,
) -> None:
    client_id = shift_row[7]
    if not client_id:
        return
    shift_id = int(task[1])
    title = task[2] or "Задача"
    body = (
        f"\u2705 *Задача выполнена*\n\n"
        f"Смена #{shift_id}\n"
        f"*{title}*\n\n"
        f"*Комментарий исполнителя:*\n{report_text or '—'}\n\n"
        "Раздел \u00abМои задачи\u00bb \u2192 при желании поставьте оценку (\u2b50)."
    )
    cid = int(client_id)
    try:
        if photo_id:
            await bot.send_photo(cid, photo_id, caption=body[:1024], parse_mode="Markdown")
        else:
            await bot.send_message(cid, body, parse_mode="Markdown")
    except Exception:
        await bot.send_message(cid, body, parse_mode="Markdown")


@router.callback_query(F.data == "my_client_tasks")
async def client_tasks_panel(callback: types.CallbackQuery):
    await _render_client_tasks(callback, "all")


@router.callback_query(F.data == "client_add_task_pick_shift")
async def client_add_task_pick_shift(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    shifts = list_shifts_for_client(user_id)
    if not shifts:
        await callback.message.edit_text(
            "❌ У вас нет смен. Сначала создайте смену через администратора.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    for s in shifts[:20]:
        rows.append([InlineKeyboardButton(text=f"📅 #{s[0]} {format_date_ru(s[1])} {s[2]}-{s[3]}", callback_data=f"add_task_{s[0]}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    await callback.message.edit_text(
        "📝 Выберите смену, куда добавить задачу:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_rate_val_"))
async def client_rate_task_save(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if not get_client(uid):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    rest = (callback.data or "").replace("client_rate_val_", "", 1)
    if "_" not in rest:
        await callback.answer()
        return
    task_id_s, rating_s = rest.rsplit("_", 1)
    if not task_id_s.isdigit() or not rating_s.isdigit():
        await callback.answer()
        return
    task_id = int(task_id_s)
    rating = int(rating_s)
    ok = set_task_client_rating(uid, task_id, rating)
    if ok:
        await callback.message.edit_text(
            f"Спасибо! Оценка {rating}/5 сохранена.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Мои задачи", callback_data="my_client_tasks")]
                ]
            ),
        )
        await callback.answer("Сохранено.")
    else:
        await callback.answer(
            "Не удалось сохранить: уже оценено, задача не выполнена или нет доступа.",
            show_alert=True,
        )


@router.callback_query(F.data.startswith("client_rate_task_"))
async def client_rate_task_pick(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if not get_client(uid):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    raw = (callback.data or "").replace("client_rate_task_", "", 1)
    if not raw.isdigit():
        await callback.answer()
        return
    task_id = int(raw)
    if not client_owns_task(uid, task_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task = get_task(task_id)
    if not task or str(task[5]) != "completed":
        await callback.answer("Задача ещё не выполнена.", show_alert=True)
        return
    if len(task) > 9 and task[9] is not None:
        await callback.answer("Оценка уже поставлена.", show_alert=True)
        return
    title = (task[2] or "Задача")[:60]
    await callback.message.edit_text(
        f"Оцените выполнение задачи (1–5):\n*{title}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="1", callback_data=f"client_rate_val_{task_id}_1"),
                    InlineKeyboardButton(text="2", callback_data=f"client_rate_val_{task_id}_2"),
                    InlineKeyboardButton(text="3", callback_data=f"client_rate_val_{task_id}_3"),
                    InlineKeyboardButton(text="4", callback_data=f"client_rate_val_{task_id}_4"),
                    InlineKeyboardButton(text="5", callback_data=f"client_rate_val_{task_id}_5"),
                ],
                [InlineKeyboardButton(text="Отмена", callback_data="my_client_tasks")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("my_client_tasks_"))
async def client_tasks_panel_filter(callback: types.CallbackQuery):
    flt = callback.data.replace("my_client_tasks_", "")
    if flt not in {"all", "open", "done"}:
        flt = "all"
    await _render_client_tasks(callback, flt)


async def _render_client_tasks(callback: types.CallbackQuery, flt: str) -> None:
    user_id = callback.from_user.id
    if not get_client(user_id):
        await callback.answer("Только для заказчика.", show_alert=True)
        return
    tasks_all = list_tasks_for_client(user_id, limit=200)
    if flt == "open":
        tasks = [t for t in tasks_all if str(t[2]) != "completed"]
    elif flt == "done":
        tasks = [t for t in tasks_all if str(t[2]) == "completed"]
    else:
        tasks = tasks_all
    if not tasks:
        await callback.message.edit_text(
            "✅ По выбранному фильтру задач нет.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Все", callback_data="my_client_tasks_all"),
                        InlineKeyboardButton(text="Открытые", callback_data="my_client_tasks_open"),
                        InlineKeyboardButton(text="Выполненные", callback_data="my_client_tasks_done"),
                    ],
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
                ]
            ),
        )
        await callback.answer()
        return
    open_cnt = sum(1 for t in tasks_all if str(t[2]) != "completed")
    done_cnt = len(tasks_all) - open_cnt
    text = (
        "✅ *Мои задачи (заказчик)*\n\n"
        f"Всего: *{len(tasks_all)}* | Открыто: *{open_cnt}* | Выполнено: *{done_cnt}*\n"
        f"Фильтр: *{ {'all':'Все','open':'Открытые','done':'Выполненные'}[flt] }*\n\n"
    )
    rows = []
    for task_id, title, status, shift_id, date, st, et, worker_name, client_rating in tasks[:40]:
        emoji = "✅" if status == "completed" else "⏳"
        d_ru = format_date_ru(date)
        text += f"{emoji} #{task_id} | {title}\nСмена #{shift_id}: {d_ru} {st}-{et} | {worker_name}\n"
        if status == "completed":
            if client_rating is not None:
                text += f"   Оценка: {client_rating}/5\n"
            else:
                text += "   Оценка: можно поставить кнопкой ниже\n"
        text += "\n"
        if status == "completed" and client_rating is None:
            short = (title or "")[:28]
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"\u2b50 Оценить: {short}",
                        callback_data=f"client_rate_task_{task_id}",
                    )
                ]
            )
    # Быстрые кнопки по сменам, где есть открытые задачи
    shift_ids = []
    for t in tasks:
        if str(t[2]) != "completed" and int(t[3]) not in shift_ids:
            shift_ids.append(int(t[3]))
    for sid in shift_ids[:8]:
        rows.append([InlineKeyboardButton(text=f"📅 Смена #{sid}", callback_data=f"shift_detail_{sid}")])
    rows.extend(
        [
            [
                InlineKeyboardButton(text="Все", callback_data="my_client_tasks_all"),
                InlineKeyboardButton(text="Открытые", callback_data="my_client_tasks_open"),
                InlineKeyboardButton(text="Выполненные", callback_data="my_client_tasks_done"),
            ],
            [
                InlineKeyboardButton(text="📋 Проекты", callback_data="my_projects"),
                InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts"),
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
        ]
    )
    await callback.message.edit_text(
        text[:3900],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


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
        text += f"• {format_date_ru(s[1])} {s[2]}-{s[3]}\n"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{format_date_ru(s[1])} → задачи",
                    callback_data=f"tasks_{s[0]}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    rows.append(
        [
            InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts"),
            InlineKeyboardButton(text="🏠 Меню", callback_data="main_menu"),
        ]
    )
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

    keyboard_rows = [
        [InlineKeyboardButton(text="Всем на смене", callback_data="assign_mode_all")],
        [InlineKeyboardButton(text="Несколько человек", callback_data="assign_mode_multi")],
        [InlineKeyboardButton(text="Один исполнитель", callback_data="assign_mode_one")],
        [InlineKeyboardButton(text="Без исполнителя", callback_data="assign_task_skip")],
    ]
    await message.answer(
        "Кому поставить задачу? Можно всем сразу, нескольким, одному или пока без исполнителя.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
    )
    await state.set_state(TaskCreation.choose_assignment)


@router.callback_query(TaskCreation.choose_assignment, F.data == "assign_mode_all")
async def assign_mode_all_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    title = data["task_title"]
    description = data.get("task_description", "")
    assignments = get_shift_assignments(shift_id)
    ids = _assignment_worker_ids(assignments)
    if not ids:
        await callback.answer("Нет исполнителей на смене.", show_alert=True)
        return
    shift = get_shift(shift_id)
    d_fmt = format_date_ru(shift[2])
    pairs: list[tuple[int, int]] = []
    for wid in ids:
        tid = create_task(shift_id, title, description, wid)
        pairs.append((wid, tid))
    await _notify_workers_new_tasks(
        callback.bot,
        shift_id=shift_id,
        shift_date_fmt=d_fmt,
        title=title,
        description=description,
        worker_task_pairs=pairs,
    )
    await callback.message.edit_text(
        f"Создано задач: {len(pairs)} — по одной каждому на смене.\n\n📋 {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )
    await state.clear()
    await callback.answer()


@router.callback_query(TaskCreation.choose_assignment, F.data == "assign_mode_one")
async def assign_mode_one_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    assignments = get_shift_assignments(shift_id)
    keyboard_rows = []
    for a in assignments:
        wid = int(a[2])
        name = assignment_join_worker_name(a)
        keyboard_rows.append([InlineKeyboardButton(text=name[:40], callback_data=f"assign_task_{wid}")])
    keyboard_rows.append([InlineKeyboardButton(text="Без исполнителя", callback_data="assign_task_skip")])
    await callback.message.edit_text(
        "Выберите одного исполнителя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
    )
    await state.set_state(TaskCreation.assigned_to)
    await callback.answer()


@router.callback_query(TaskCreation.choose_assignment, F.data == "assign_mode_multi")
async def assign_mode_multi_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    assignments = get_shift_assignments(shift_id)
    await state.update_data(task_picked=[])
    await callback.message.edit_text(
        "Отметьте исполнителей (нажмите ещё раз, чтобы снять отметку). "
        "Затем нажмите «Создать для выбранных».",
        reply_markup=_multi_pick_keyboard(assignments, []),
    )
    await state.set_state(TaskCreation.pick_workers)
    await callback.answer()


@router.callback_query(TaskCreation.choose_assignment, F.data == "assign_task_skip")
async def assign_skip_from_choice(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    title = data["task_title"]
    description = data.get("task_description", "")
    create_task(shift_id, title, description, None)
    await callback.message.edit_text(
        f"Задача создана без исполнителя.\n\n📋 {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )
    await state.clear()
    await callback.answer()


@router.callback_query(TaskCreation.pick_workers, F.data.startswith("assign_toggle_"))
async def assign_toggle_worker(callback: types.CallbackQuery, state: FSMContext):
    wid = int(callback.data.replace("assign_toggle_", ""))
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    picked = list(data.get("task_picked") or [])
    if wid in picked:
        picked = [x for x in picked if x != wid]
    else:
        picked = [*picked, wid]
    await state.update_data(task_picked=picked)
    assignments = get_shift_assignments(shift_id)
    await callback.message.edit_reply_markup(reply_markup=_multi_pick_keyboard(assignments, picked))
    await callback.answer()


@router.callback_query(TaskCreation.pick_workers, F.data == "assign_multi_done")
async def assign_multi_done(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    picked = list(dict.fromkeys(int(x) for x in (data.get("task_picked") or [])))
    if not picked:
        await callback.answer("Выберите хотя бы одного исполнителя.", show_alert=True)
        return
    shift_id = int(data["task_shift_id"])
    title = data["task_title"]
    description = data.get("task_description", "")
    shift = get_shift(shift_id)
    d_fmt = format_date_ru(shift[2])
    pairs: list[tuple[int, int]] = []
    for wid in picked:
        tid = create_task(shift_id, title, description, wid)
        pairs.append((wid, tid))
    await _notify_workers_new_tasks(
        callback.bot,
        shift_id=shift_id,
        shift_date_fmt=d_fmt,
        title=title,
        description=description,
        worker_task_pairs=pairs,
    )
    await callback.message.edit_text(
        f"Создано задач: {len(pairs)}.\n\n📋 {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )
    await state.clear()
    await callback.answer()


@router.callback_query(TaskCreation.pick_workers, F.data == "assign_multi_cancel")
async def assign_multi_cancel(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    await state.clear()
    await callback.message.edit_text(
        "Создание задачи отменено.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )
    await callback.answer()


@router.callback_query(TaskCreation.assigned_to, F.data.startswith("assign_task_"))
async def task_assign_received(callback: types.CallbackQuery, state: FSMContext):
    worker_id = None if callback.data == "assign_task_skip" else int(callback.data.replace("assign_task_", ""))
    data = await state.get_data()
    shift_id = int(data["task_shift_id"])
    title = data["task_title"]
    description = data.get("task_description", "")

    task_id = create_task(shift_id, title, description, worker_id)

    await callback.message.edit_text(
        f"✅ Задача создана!\n\n📋 {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="К смене", callback_data=f"shift_detail_{shift_id}")]
            ]
        ),
    )

    if worker_id:
        shift = get_shift(shift_id)
        await _notify_workers_new_tasks(
            callback.bot,
            shift_id=shift_id,
            shift_date_fmt=format_date_ru(shift[2]),
            title=title,
            description=description,
            worker_task_pairs=[(worker_id, task_id)],
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
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="📅 Мои смены", callback_data="my_shifts"),
            InlineKeyboardButton(text="🏠 Меню", callback_data="main_menu"),
        ]
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("complete_task_"))
async def complete_task_start(callback: types.CallbackQuery, state: FSMContext):
    task_id = int(callback.data.replace("complete_task_", ""))
    await state.update_data(completing_task_id=task_id)
    await callback.message.edit_text(
        "📝 *ОТЧЁТ*\n\nСначала *текстом*: что сделано (или `-` без комментария).\n"
        "Следующим сообщением бот попросит *фото-подтверждение*.",
        parse_mode="Markdown",
    )
    await state.set_state(TaskCompletion.report_text)
    await callback.answer()


@router.message(TaskCompletion.report_text)
async def task_report_text_received(message: types.Message, state: FSMContext):
    raw = (message.text or message.caption or "").strip()
    if not raw:
        await message.answer(
            "Нужен короткий текст отчёта. Напишите, что сделано, или `-` без комментария. "
            "Если отправили только фото — добавьте подпись к фото или отдельным сообщением текст."
        )
        return
    report_text = raw
    if report_text == "-":
        report_text = ""
    await state.update_data(report_text=report_text)
    await message.answer(
        "📸 Отправьте фото, как сделано (заказчик увидит его в уведомлении). "
        "Если фото совсем нельзя — отправьте `0`."
    )
    await state.set_state(TaskCompletion.report_photo)


@router.message(TaskCompletion.report_photo, F.photo)
async def task_report_photo_received(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    task_id = data["completing_task_id"]
    report_text = data.get("report_text", "")
    complete_task(task_id, report_text, photo_id)
    await message.answer("✅ Задача выполнена!")
    task = get_task(int(task_id))
    if task:
        shift_id = int(task[1])
        shift_row = get_shift_with_owner(shift_id)
        if shift_row:
            await _notify_client_task_completed(
                message.bot, shift_row, task, report_text, photo_id
            )
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)
async def task_report_skip_photo(message: types.Message, state: FSMContext):
    if message.text == "0":
        data = await state.get_data()
        task_id = data["completing_task_id"]
        report_text = data.get("report_text", "")
        complete_task(task_id, report_text, None)
        await message.answer("✅ Задача выполнена!")
        task = get_task(int(task_id))
        if task:
            shift_id = int(task[1])
            shift_row = get_shift_with_owner(shift_id)
            if shift_row:
                await _notify_client_task_completed(
                    message.bot, shift_row, task, report_text, None
                )
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")
