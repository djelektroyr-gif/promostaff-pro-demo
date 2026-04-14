# handlers/shift_center.py — center of shift (timeline + traffic light) and project hub.
from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_USER_ID
from db import (
    get_shift_report,
    get_client,
    get_worker,
    client_owns_shift,
    client_owns_project,
    worker_assigned_to_project,
    get_project,
    get_shifts_by_project,
    get_assignment,
    format_date_ru,
)
from services.shift_hub import format_shift_hub

router = Router()


def _hub_keyboard(shift_id: int, *, viewer: str, project_id: int | None) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="\u0427\u0430\u0442 \u0441\u043c\u0435\u043d\u044b",
                callback_data=f"chat_{shift_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"shift_hub_{viewer}_{shift_id}",
            )
        ],
    ]
    if project_id is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u041a \u043f\u0440\u043e\u0435\u043a\u0442\u0443",
                    callback_data=f"project_hub_{project_id}",
                )
            ]
        )
    if viewer == "cl":
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u0441\u043c\u0435\u043d\u044b",
                    callback_data=f"shift_detail_{shift_id}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u041c\u043e\u0438 \u0441\u043c\u0435\u043d\u044b",
                    callback_data="my_shifts",
                )
            ]
        )
    elif viewer == "wk":
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u041c\u043e\u044f \u0441\u043c\u0435\u043d\u0430",
                    callback_data=f"worker_shift_{shift_id}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u041c\u043e\u0438 \u0441\u043c\u0435\u043d\u044b",
                    callback_data="my_shifts",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="\u0423\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0441\u043c\u0435\u043d\u0430\u043c\u0438",
                    callback_data="admin_shift_manage",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="\u041c\u0435\u043d\u044e", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_shift_hub(data: str) -> tuple[str, int] | None:
    for prefix, viewer in (
        ("shift_hub_cl_", "cl"),
        ("shift_hub_ad_", "ad"),
        ("shift_hub_wk_", "wk"),
    ):
        if data.startswith(prefix):
            return viewer, int(data.replace(prefix, ""))
    return None


@router.callback_query(lambda c: _parse_shift_hub(c.data or "") is not None)
async def shift_hub_open(callback: CallbackQuery):
    parsed = _parse_shift_hub(callback.data or "")
    if not parsed:
        await callback.answer()
        return
    viewer, shift_id = parsed
    uid = callback.from_user.id

    if viewer == "cl":
        if not get_client(uid) or not client_owns_shift(uid, shift_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return
        include_pay = False
    elif viewer == "ad":
        if uid != int(ADMIN_USER_ID):
            await callback.answer("Нет доступа.", show_alert=True)
            return
        include_pay = True
    else:
        if not get_worker(uid) or not get_assignment(shift_id, uid):
            await callback.answer("Нет доступа.", show_alert=True)
            return
        include_pay = False

    rep_full = get_shift_report(shift_id)
    if viewer == "wk":
        rep = {
            **rep_full,
            "assignments": [a for a in (rep_full.get("assignments") or []) if int(a[2]) == int(uid)],
        }
    else:
        rep = rep_full

    shift = rep.get("shift")
    project_id = int(shift[1]) if shift and shift[1] is not None else None

    text = format_shift_hub(rep, include_pay=include_pay)
    await callback.message.edit_text(
        text,
        reply_markup=_hub_keyboard(shift_id, viewer=viewer, project_id=project_id),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("project_hub_"))
async def project_hub_open(callback: CallbackQuery):
    pid = int(callback.data.replace("project_hub_", ""))
    uid = callback.from_user.id
    is_ad = uid == int(ADMIN_USER_ID)
    is_cl = bool(get_client(uid) and client_owns_project(uid, pid))
    is_wk = bool(get_worker(uid) and worker_assigned_to_project(uid, pid))
    if not (is_ad or is_cl or is_wk):
        await callback.answer("Нет доступа к проекту.", show_alert=True)
        return

    pr = get_project(pid)
    if not pr:
        await callback.answer("Проект не найден.", show_alert=True)
        return
    pname = pr[1]
    shifts = get_shifts_by_project(pid)
    text = f"*\u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043f\u043e \u043f\u0440\u043e\u0435\u043a\u0442\u0443 #{pid}*\n{pname}\n\n*\u0421\u043c\u0435\u043d\u044b:*\n"
    if not shifts:
        text += "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u0441\u043c\u0435\u043d.\n"
    rows = []
    hub_cb = "shift_hub_ad" if is_ad else "shift_hub_cl" if is_cl else "shift_hub_wk"
    for s in shifts[:20]:
        sid = int(s[0])
        text += f"• #{sid} {format_date_ru(s[2])} {s[3]}-{s[4]}\n"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"\u0421\u043c\u0435\u043d\u0430 #{sid} \u2192 \u043f\u043e\u0434\u0440\u043e\u0431\u043d\u043e",
                    callback_data=f"{hub_cb}_{sid}",
                ),
                InlineKeyboardButton(text="\u0427\u0430\u0442", callback_data=f"chat_{sid}"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="\u0427\u0430\u0442 \u043f\u0440\u043e\u0435\u043a\u0442\u0430",
                callback_data=f"proj_chat_{pid}",
            )
        ]
    )
    back = "my_projects" if is_cl else "admin_back" if is_ad else "main_menu"
    rows.append([InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data=back)])
    await callback.message.edit_text(
        text[:3900],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()
