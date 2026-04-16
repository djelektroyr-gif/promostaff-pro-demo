# handlers/tasks.py
import logging

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import PARSE_MODE_TELEGRAM, is_admin_user
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
    client_owns_shift,
    set_task_client_rating,
)
from services.text_utils import bold, escape_markdown as em
from services.admin_broadcast import send_all_admins
from states import TaskCreation, TaskCompletion

router = Router()
logger = logging.getLogger(__name__)

T_STATUS = 5


def _task_flow_actions_keyboard(shift_id: int, is_admin: bool) -> InlineKeyboardMarkup:
    if is_admin:
        rows = [
            [InlineKeyboardButton(text="🎯 Сводка смены", callback_data=f"shift_hub_ad_{shift_id}")],
            [InlineKeyboardButton(text="📝 Ещё задача", callback_data=f"add_task_{shift_id}")],
            [InlineKeyboardButton(text="🗓 Управление сменами", callback_data="admin_shift_manage")],
        ]
    else:
        rows = [
            [InlineKeyboardButton(text="📅 Открыть эту смену", callback_data=f"shift_detail_{shift_id}")],
            [InlineKeyboardButton(text="📝 Ещё одна задача", callback_data="client_add_task_pick_shift")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
        ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
                text=f"✅ Готово: создать ({len(picked)} чел.)",
                callback_data="assign_multi_done",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отмена",
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
            body = (
                bold("НОВАЯ ЗАДАЧА")
                + "\n\n"
                + em(f"Смена #{shift_id} ({shift_date_fmt})")
                + "\n\n"
                + bold(title)
                + "\n"
                + em(description)
            )
            await bot.send_message(
                worker_id,
                body,
                parse_mode=PARSE_MODE_TELEGRAM,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="\U0001f4dd Отчитаться",
                                callback_data=f"complete_task_{task_id}",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text="Открыть смену",
                                callback_data=f"worker_shift_{shift_id}",
                            )
                        ],
                    ]
                ),
            )
        except Exception as e:
            logger.warning(
                "_notify_workers_new_tasks failed worker_id=%s task_id=%s: %s",
                worker_id,
                task_id,
                e,
                exc_info=True,
            )


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
        bold("Задача выполнена")
        + "\n\n"
        + em(f"Смена #{shift_id}")
        + "\n"
        + bold(title)
        + "\n\n"
        + bold("Комментарий исполнителя:")
        + "\n"
        + em(report_text or "—")
        + "\n\n"
        + em("Раздел «Мои задачи» → при желании поставьте оценку (⭐).")
    )
    cid = int(client_id)
    try:
        if photo_id:
            await bot.send_photo(cid, photo_id, caption=body[:1024], parse_mode=PARSE_MODE_TELEGRAM)
        else:
            await bot.send_message(cid, body, parse_mode=PARSE_MODE_TELEGRAM)
    except Exception as e:
        logger.warning("_notify_client_task_completed failed cid=%s: %s", cid, e, exc_info=True)
        await bot.send_message(cid, body, parse_mode=PARSE_MODE_TELEGRAM)
    try:
        await send_all_admins(
            bot,
            f"✅ Выполнена задача по смене #{shift_id}\n\n{title}\nКомментарий: {report_text or '—'}",
            parse_mode=None,
        )
    except Exception as e:
        logger.warning("_notify_client_task_completed to admins failed shift_id=%s: %s", shift_id, e, exc_info=True)


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
            "❌ Пока нет ваших смен\n\n"
            "Смену создаёт администратор PROMOSTAFF. Как только смена появится — "
            "зайдите снова: «Главное меню» → «Задачи» → «Новая задача для исполнителя».",
            parse_mode=None,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu")]]
            ),
        )
        await callback.answer()
        return
    rows = []
    for s in shifts[:20]:
        rows.append([InlineKeyboardButton(text=f"📅 #{s[0]} {format_date_ru(s[1])} {s[2]}-{s[3]}", callback_data=f"add_task_{s[0]}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    await callback.message.edit_text(
        "📝 Новая задача — шаг 1 из 4\n\n"
        "Нажмите на смену ниже (дата и время). Дальше бот попросит:\n"
        "2) короткое название задачи;\n"
        "3) описание (можно прочерк -);\n"
        "4) кому из исполнителей на смене её дать.\n\n"
        "На смену должны быть уже назначены люди — иначе задачу некому отправить.",
        parse_mode=None,
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
        f"Оцените выполнение задачи (1–5):\n{title}",
        parse_mode=None,
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
            "✅ По этому фильтру задач нет.\n\n"
            "Чтобы поставить новую: меню «Задачи» → кнопка "
            "«Новая задача для исполнителя» → выберите смену.",
            parse_mode=None,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Все", callback_data="my_client_tasks_all"),
                        InlineKeyboardButton(text="Открытые", callback_data="my_client_tasks_open"),
                        InlineKeyboardButton(text="Выполненные", callback_data="my_client_tasks_done"),
                    ],
                    [
                        InlineKeyboardButton(
                            text="📝 Поставить новую задачу",
                            callback_data="client_add_task_pick_shift",
                        )
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
        "✅ Мои задачи (заказчик)\n\n"
        "Новая задача: меню «Задачи» → «Новая задача для исполнителя».\n\n"
        f"Всего: {len(tasks_all)} | Открыто: {open_cnt} | Выполнено: {done_cnt}\n"
        f"Фильтр: { {'all':'Все','open':'Открытые','done':'Выполненные'}[flt] }\n\n"
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
                InlineKeyboardButton(
                    text="📝 Новая задача исполнителю",
                    callback_data="client_add_task_pick_shift",
                )
            ],
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
        parse_mode=None,
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
    text = "📋 Смены с незавершёнными задачами:\n\n"
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
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("add_task_"))
async def add_task_start(callback: types.CallbackQuery, state: FSMContext):
    shift_id = int(callback.data.replace("add_task_", ""))
    uid = callback.from_user.id
    is_admin = is_admin_user(int(uid))
    if not is_admin and (not get_client(uid) or not client_owns_shift(uid, shift_id)):
        await callback.answer(
            "Задачу может поставить заказчик этой смены или администратор. Откройте смену из «Мои смены» или сводку админа.",
            show_alert=True,
        )
        return
    await state.update_data(task_shift_id=shift_id)
    intro = (
        "📋 Новая задача — шаг 2 из 4\n\n"
        "Напишите одним сообщением короткое название, как в списке дел.\n"
        "Пример: Разложить флаеры у входа"
    )
    if is_admin:
        intro += "\n\nВы администратор — задачу увидят выбранные исполнители, как если бы её поставил заказчик."
    await callback.message.edit_text(
        intro,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
    await state.set_state(TaskCreation.title)
    await callback.answer()


@router.message(TaskCreation.title)
async def task_title_received(message: types.Message, state: FSMContext):
    await state.update_data(task_title=message.text.strip())
    await message.answer(
        "📋 Шаг 3 из 4 — описание\n\n"
        "Напишите подробности одним сообщением. Если подробностей нет — отправьте один символ: -",
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
    )
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
        await message.answer(
            "❌ На эту смену ещё не назначены исполнители. Попросите администратора назначить людей, "
            "затем снова: «Новая задача для исполнителя».",
            parse_mode=None,
        )
        await state.clear()
        return

    keyboard_rows = [
        [InlineKeyboardButton(text="✅ Всем на смене сразу", callback_data="assign_mode_all")],
        [InlineKeyboardButton(text="👥 Выбрать нескольких", callback_data="assign_mode_multi")],
        [InlineKeyboardButton(text="👤 Одному человеку", callback_data="assign_mode_one")],
        [InlineKeyboardButton(text="📌 Пока без исполнителя (черновик)", callback_data="assign_task_skip")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")],
    ]
    await message.answer(
        "📋 Шаг 4 из 4 — кому отправить задачу\n\n"
        "Выберите одну кнопку ниже. Исполнители сразу получат уведомление в Telegram.",
        parse_mode=None,
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
    is_admin = is_admin_user(int(callback.from_user.id))
    await callback.message.edit_text(
        f"✅ Готово! Создано задач: {len(pairs)} (каждому на смене).\n\n📋 {title}\n\n"
        "Исполнители получили уведомление. Вы можете вернуться в карточку смены или поставить ещё одну задачу.",
        parse_mode=None,
        reply_markup=_task_flow_actions_keyboard(shift_id, is_admin),
    )
    await state.clear()
    await callback.answer(f"✅ Задача отправлена: {len(pairs)} исполнителям.", show_alert=True)


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
    keyboard_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")])
    await callback.message.edit_text(
        "👤 Кому одному?\n\nНажмите на фамилию исполнителя. «Без исполнителя» — если пока только фиксируете задачу в системе.",
        parse_mode=None,
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
        "👥 Несколько исполнителей\n\n"
        "Нажимайте по строкам — галочка ✓ появится у выбранных. "
        "Потом внизу нажмите «Готово: создать задачи».",
        parse_mode=None,
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
    is_admin = is_admin_user(int(callback.from_user.id))
    await callback.message.edit_text(
        f"📌 Задача сохранена без исполнителя (никому не ушло уведомление).\n\n📋 {title}\n\n"
        "Позже можно создать задачу заново и назначить людей.",
        parse_mode=None,
        reply_markup=_task_flow_actions_keyboard(shift_id, is_admin),
    )
    await state.clear()
    await callback.answer("✅ Задача сохранена как черновик.", show_alert=True)


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
    is_admin = is_admin_user(int(callback.from_user.id))
    await callback.message.edit_text(
        f"✅ Готово! Создано задач: {len(pairs)}.\n\n📋 {title}",
        parse_mode=None,
        reply_markup=_task_flow_actions_keyboard(shift_id, is_admin),
    )
    await state.clear()
    await callback.answer(f"✅ Задача отправлена: {len(pairs)} исполнителям.", show_alert=True)


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

    is_admin = is_admin_user(int(callback.from_user.id))
    await callback.message.edit_text(
        f"✅ Задача создана!\n\n📋 {title}",
        parse_mode=None,
        reply_markup=_task_flow_actions_keyboard(shift_id, is_admin),
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
    if worker_id:
        await callback.answer("✅ Задача отправлена исполнителю.", show_alert=True)
    else:
        await callback.answer("✅ Задача сохранена без исполнителя.", show_alert=True)


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

    text = "📋 ВАШИ ЗАДАЧИ\n\n"
    keyboard_rows = []
    for t in tasks:
        status_emoji = "✅" if t[T_STATUS] == "completed" else "⏳"
        text += f"{status_emoji} {t[2]}\n"
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
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode=None)
    await callback.answer()


@router.callback_query(F.data.startswith("complete_task_"))
async def complete_task_start(callback: types.CallbackQuery, state: FSMContext):
    task_id = int(callback.data.replace("complete_task_", ""))
    await state.update_data(completing_task_id=task_id)
    await callback.message.edit_text(
        "📝 ОТЧЁТ\n\nСначала текстом: что сделано (или - без комментария).\n"
        "Следующим сообщением бот попросит фото-подтверждение.",
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
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
        "Если фото совсем нельзя — отправьте `0`.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow")]]
        ),
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
