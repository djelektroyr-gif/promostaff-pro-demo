# -*- coding: utf-8 -*-
"""Splice edits into handlers/tasks.py without embedding emoji in this script."""
from pathlib import Path

p = Path(__file__).resolve().parent / "handlers" / "tasks.py"
lines = p.read_text(encoding="utf-8").splitlines(keepends=True)

def find_line(sub: str) -> int:
    for i, ln in enumerate(lines):
        if sub in ln:
            return i
    return -1

# 1) add_task pick shift: after "for s in shifts[:20]:"
i = find_line("for s in shifts[:20]:")
if i < 0:
    raise SystemExit("for s in shifts")
old_append = lines[i + 1]
if "d_ru" not in old_append:
    lines[i + 1] = "        d_ru = format_date_ru(s[1])\n"
    lines.insert(i + 2, old_append.replace("{s[1]}", "{d_ru}", 1))

# 2) client tasks loop — replace 3-line block
i = find_line("for task_id, title, status, shift_id, date, st, et, worker_name in tasks[:40]:")
if i < 0:
    raise SystemExit("task loop")
if "client_rating" not in lines[i]:
    del lines[i : i + 3]
    block = """    for task_id, title, status, shift_id, date, st, et, worker_name, client_rating in tasks[:40]:
        emoji = "\u2705" if status == "completed" else "\u23f3"
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
"""
    for j, bl in enumerate(block.splitlines(keepends=True)):
        lines.insert(i + j, bl)

# 3) "Открыть смену" button
p.read_text  # no-op
text = "".join(lines)
text = text.replace("Открыть смену #", "Смена #", 1)
lines = text.splitlines(keepends=True)

# 4) complete_task_start prompt
text = "".join(lines)
old_prompt = """    await callback.message.edit_text(
        "\U0001f4dd *ОТЧ��Т*\n\nОпишите, что сделано (или `-`):",
        parse_mode="Markdown",
    )"""
new_prompt = """    await callback.message.edit_text(
        "\U0001f4dd *ОТЧ��Т*\n\n"
        "Сначала *текстом*: что сделано (или `-` без комментария).\n"
        "Следующим сообщением бот попросит *фото-подтверждение*.",
        parse_mode="Markdown",
    )"""
if old_prompt in text:
    text = text.replace(old_prompt, new_prompt, 1)
else:
    # fallback: emoji variant from file
    text = text.replace(
        "Опишите, что сделано (или `-`):",
        "Сначала *текстом*: что сделано (или `-` без комментария).\n"
        "Следующим сообщением бот попросит *фото-подтверждение*.",
        1,
    )
lines = text.splitlines(keepends=True)

# 5) task_report_text_received — replace whole function body by markers
text = "".join(lines)
start = text.find("async def task_report_text_received")
if start < 0:
    raise SystemExit("task_report_text_received")
end = text.find("@router.message(TaskCompletion.report_photo, F.photo)", start)
if end < 0:
    raise SystemExit("end task_report")
new_fn = """@router.message(TaskCompletion.report_text)
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
    await message.answer("\\U0001f4f8 Отправьте фото выполнения (или `0` если фото не будет):")
    await state.set_state(TaskCompletion.report_photo)


"""
# find start of decorator
d0 = text.rfind("@router.message(TaskCompletion.report_text)", 0, start)
text = text[:d0] + new_fn + text[end:]
lines = text.splitlines(keepends=True)

# Fix escaped unicode in answer stringtext = "".join(lines)
text = text.replace('"\\U0001f4f8 ', '"\U0001f4f8 ')  # wrong - need real fix
text = text.replace('"\\U0001f4f8',�')lines = text.splitlines(keepends=True)

# 6) replace send_message client notify with helper — photo handler
text = "".join(lines)
old_snip = """        if shift_row and shift_row[7]:
            await message.bot.send_message(
                int(shift_row[7]),
                f"�� Исполнитель завершил задачу по смене #{shift_id}: {task[2]}",
            )
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)"""
new_snip = """        if shift_row and shift_row[7]:
            await _notify_client_task_completed(message.bot, shift_row, task, report_text, photo_id)
    await state.clear()


@router.message(TaskCompletion.report_photo, F.text)"""
if old_snip in text:
    text = text.replace(old_snip, new_snip, 1)
lines = text.splitlines(keepends=True)

# 7) skip photo branch
text = "".join(lines)
old_snip2 = """            if shift_row and shift_row[7]:
                await message.bot.send_message(
                    int(shift_row[7]),
� Исполнитель завершил задачу по смене #{shift_id}: {task[2]}",
                )
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")"""
new_snip2 = """            if shift_row and shift_row[7]:
                await _notify_client_task_completed(message.bot, shift_row, task, report_text, None)
        await state.clear()
    else:
        await message.answer("Отправьте фото или `0`.")"""
if old_snip2 in text:
    text = text.replace(old_snip2, new_snip2, 1)
lines = text.splitlines(keepends=True)

# 8) append rating handlers if missing
text = "".join(lines)
if "client_rate_val_" not in text:
    text = text.rstrip() + """

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
        f"\u2b50 Оцените задачу #{task_id} \u00ab{task[2]}\u00bb\\n\\n"
        "1 \u2014 плохо, 5 \u2014 отлично:",
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
        await callback.message.edit_text(f"\u2705 Спасибо! Оценка {rating}/5 сохранена.")
    else:
        await callback.message.edit_text("\u274c Не удалось сохранить (уже есть оценка или нет доступа).")
    await callback.answer()
"""

p.write_text(text, encoding="utf-8")
print("splice done")
