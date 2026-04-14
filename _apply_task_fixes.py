# -*- coding: utf-8 -*-
from pathlib import Path

p = Path(__file__).resolve().parent / "handlers" / "tasks.py"
t = p.read_text(encoding="utf-8")

repls = [
    (
        """    for s in shifts[:20]:
        rows.append([InlineKeyboardButton(text=f"�� #{s[0]} {s[1]} {s[2]}-{s[3]}", callback_data=f"add_task_{s[0]}")])""",
 """    for s in shifts[:20]:
        d_ru = format_date_ru(s[1])
        rows.append([InlineKeyboardButton(text=f"�� #{s[0]} {d_ru} {s[2]}-{s[3]}", callback_data=f"add_task_{s[0]}")])""",
    ),
    (
        """    for task_id, title, status, shift_id, date, st, et, worker_name in tasks[:40]:
        emoji = "��" if status == "completed" else "���"
        text += f"{emoji} #{task_id} | {title}\\nСмена #{shift_id}: {date} {st}-{et} | {worker_name}\\n\\n""",
        """    for task_id, title, status, shift_id, date, st, et, worker_name, client_rating in tasks[:40]:
        emoji = "��" if status == "completed" else "���"
        d_ru = format_date_ru(date)
        text += f"{emoji} #{task_id} | {title}\\nСмена #{shift_id}: {d_ru} {st}-{et} | {worker_name}\\n"
        if status == "completed":
            if client_rating is not None:
                text += f"   Оценка: {client_rating}/5\\n"
            else:
                text += "   Оценка: можно поставить кнопкой ниже\\n"
        text += "\\n"
        if status == "completed" and client_rating is None:
            short = (title or "")[:28]
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"⭐ Оценить: {short}",
                        callback_data=f"client_rate_task_{task_id}",
                    )
                ]
            )""",
    ),
    (
        """        rows.append([InlineKeyboardButton(text=f"�� Открыть смену #{sid}", callback_data=f"shift_detail_{sid}")])""",
        """        rows.append([InlineKeyboardButton(text=f"�� Смена #{sid}", callback_data=f"shift_detail_{sid}")])""",
    ),
    (
        """    await callback.message.edit_text(
        "��� *�Т*\\n\\nОпишите, что сделано (или `-`):",
        parse_mode="Markdown",
    )""",
        """    await callback.message.edit_text�� *ОТЧ��Т*\\n\\n"
        "Сначала *текстом*: что сделано (или `-` без комментария).\\n"
        "Следующим сообщением бот попросит *фото-подтверждение*.",
        parse_mode="Markdown",
    )""",
    ),
    (
        """@router.message(TaskCompletion.report_text)
async def task_report_text_received(message: types.Message, state: FSMContext):
    report_text = message.text.strip()
    if report_text == "-":
        report_text = ""
    await state.update_data(report_text=report_text)
    await message.answer("�� Отправьте фото (или `0`):")
    await state.set_state(TaskCompletion.report_photo)""",
        """@router.message(TaskCompletion.report_text)
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
    await message.answer("�� Отправьте фото выполнения (или `0` если фото не будет):")
    await state.set_state(TaskCompletion.report_photo)""",
    ),
    (
        """        if shift_row and shift_row[7]:
            await message.bot.send_message(
                int(shift_row[7]),
� Исполнитель завершил задачу по смене #{shift_id}: {task[2]}",
            )
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)""",
        """        if shift_row and shift_row[7]:
            await _notify_client_task_completed(message.bot, shift_row, task, report_text, photo_id)
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)""",
    ),
    (
        """            if shift_row and shift_row[7]:
                await message.bot.send_message(
                    int(shift_row[7]),
� Исполнитель завершил задачу по смене #{shift_id}: {task[2]}",
                )
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")""",
        """            if shift_row and shift_row[7]:
                await _notify_client_task_completed(message.bot, shift_row, task, report_text, None)
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")""",
    ),
]

for old, new in repls:
    if old not in t:
        raise SystemExit(f"MISSING:\\n{old[:80]}...")
    t = t.replace(old, new, 1)

rating_block = '''


@router.callback_query(F.data.startswith("client_rate_task_"))
async def client_rate_task_ask(callback: types.CallbackQuery):
    raw = callback.data.replace("client_rate_task_", "")
    if not raw.isdigit():
        await callback.answer()
        return
    task_id = int(raw)
    uid = callback.from_user.id
    if not get_client(uid) or not client_owns_task(uid, task_id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task = get_task(task_id)
    if not task or str(task[5]) != "completed":
        await callback.answer("Задача не завершена.", show_alert=True)
        return
    if len(task) > 9 and task[9] is not None:
        await callback.answer("Уже оценено.", show_alert=True)
        return
    stars = [
        InlineKeyboardButton(text=str(i), callback_data=f"client_rate_val_{task_id}_{i}")
        for i in range(1, 6)
    ]
    await callback.message.edit_text(
        f"⭐ Оцените задачу #{task_id} «{task[2]}»\\n\\n"
        "1 — плохо, 5 — отлично:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[stars]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_rate_val_"))
async def client_rate_task_save(callback: types.CallbackQuery):
    raw = callback.data.replace("client_rate_val_", "")
    parts = raw.split("_")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        await callback.answer()
        return
    task_id, rating = int(parts[0]), int(parts[1])
    uid = callback.from_user.id
    if set_task_client_rating(uid, task_id, rating):
        await callback.message.edit_text(f"�� Спасибо! Оценка {rating}/5 сохранена.")
    else:
        await callback.message.edit_text("�� Не удалось сохранить (уже есть оценка или нет доступа).")
    await callback.answer()
'''

if "client_rate_task_" not in t:
    t = t.rstrip() + rating_block

p.write_text(t, encoding="utf-8")
print("applied")
