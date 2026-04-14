"""Shift control center: traffic light + timeline (demo)."""
from __future__ import annotations

from datetime import datetime

from services.shift_notifier import _shift_start_end

I_ASSIGNED_NOTIFY = 11
I_REMINDER_12H = 12
I_REMINDER_3H = 13
I_ESCALATION_1H = 14
I_CHECKIN_30M = 15
I_CHECKOUT_30M = 16
I_FORGOT_CO = 17
I_EXT_REQ_AT = 21
I_EXT_RES_AT = 22
I_GEO_OK = 25
JOIN_MIN_LEN = 28

R = "\U0001F534"
Y = "\U0001F7E1"
G = "\U0001F7E2"
W = "\u26AA"


def _fmt_ts(val) -> str:
    if val is None:
        return "\u2014"
    if isinstance(val, datetime):
        return val.strftime("%d.%m %H:%M")
    s = str(val).replace("T", " ")[:16]
    return s


def _parse_ts(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None) if val.tzinfo else val
    try:
        return datetime.fromisoformat(str(val).replace(" ", "T", 1)[:19])
    except Exception:
        return None


def _worker_name(row: tuple) -> str:
    if len(row) >= 2:
        n = row[-2]
        if n:
            return str(n)
    return str(row[2])


def _geo_ok_val(row: tuple):
    if len(row) < JOIN_MIN_LEN:
        return None
    return row[I_GEO_OK]


def _geo_line(row: tuple, shift_has_fence: bool) -> str:
    ok = _geo_ok_val(row)
    if ok is None:
        return "\u0433\u0435\u043e: \u043d\u0435 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u0438\u0440\u0443\u0435\u0442\u0441\u044f" if not shift_has_fence else "\u0433\u0435\u043e: \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0435"
    if int(ok) == 1:
        return "\u0433\u0435\u043e: \u0432 \u0440\u0430\u0434\u0438\u0443\u0441\u0435"
    if int(ok) == 0:
        return "\u0433\u0435\u043e: \u0431\u044b\u043b\u0438 \u043f\u043e\u043f\u044b\u0442\u043a\u0438 \u0432\u043d\u0435 \u0440\u0430\u0434\u0438\u0443\u0441\u0430"
    return "\u0433\u0435\u043e: ?"


def _traffic_light(
    *,
    now: datetime,
    dt_start: datetime,
    status: str,
    esc_sent,
    checkin_ts,
    geo_ok,
    shift_has_fence: bool,
) -> tuple[str, str]:
    to_start = (dt_start - now).total_seconds()
    st = (status or "").lower()

    if st == "cancelled":
        return W, "\u043e\u0442\u043c\u0435\u043d\u0430 \u0434\u043b\u044f \u0438\u0441\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044f"

    if st == "checked_out":
        return G, "\u0441\u043c\u0435\u043d\u0430 \u0437\u0430\u043a\u0440\u044b\u0442\u0430"

    if st == "checked_in":
        ci = _parse_ts(checkin_ts)
        late_ci = bool(ci and ci > dt_start)
        if geo_ok is not None and int(geo_ok) == 0 and shift_has_fence:
            return R, "\u043d\u0430 \u0441\u043c\u0435\u043d\u0435, \u0433\u0435\u043e \u043d\u0435 \u043f\u0440\u043e\u0439\u0434\u0435\u043d\u043e"
        if late_ci:
            return Y, "\u043d\u0430 \u0441\u043c\u0435\u043d\u0435, \u043e\u043f\u043e\u0437\u0434\u0430\u043b \u0441 \u0447\u0435\u043a-\u0438\u043d\u043e\u043c"
        return G, "\u043d\u0430 \u0441\u043c\u0435\u043d\u0435"

    if st == "confirmed":
        ci = _parse_ts(checkin_ts)
        if now > dt_start and not ci:
            return R, "\u043d\u0435\u0442 \u0447\u0435\u043a-\u0438\u043d\u0430 \u043f\u043e\u0441\u043b\u0435 \u0441\u0442\u0430\u0440\u0442\u0430"
        if 0 < to_start <= 3600:
            return Y, "\u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b, \u0441\u043a\u043e\u0440\u043e \u0441\u0442\u0430\u0440\u0442"
        return G, "\u0432\u044b\u0445\u043e\u0434 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d"

    if st == "pending":
        if esc_sent:
            return R, "\u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b (\u044d\u0441\u043a\u0430\u043b\u0430\u0446\u0438\u044f)"
        if now >= dt_start:
            return R, "\u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b \u043a \u0441\u0442\u0430\u0440\u0442\u0443"
        if 0 < to_start <= 3600:
            return Y, "\u043d\u0435\u0442 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f (<1\u0447)"
        return Y, "\u0436\u0434\u0451\u043c \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0435"

    return W, st or "\u2014"


def format_shift_hub(
    rep: dict,
    *,
    now: datetime | None = None,
    include_pay: bool = True,
) -> str:
    shift = rep.get("shift")
    if not shift:
        return "\u0421\u043c\u0435\u043d\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430."
    now = now or datetime.now()
    shift_id = shift[0]
    shift_date = str(shift[2])
    st_t = str(shift[3] or "")
    en_t = str(shift[4] or "")
    loc = shift[5] or "\u2014"
    rate = shift[6]
    try:
        dt_start, _dt_end = _shift_start_end(shift_date, st_t, en_t)
    except Exception:
        dt_start = now

    exp_lat, exp_lng = shift[9], shift[10]
    shift_has_fence = exp_lat is not None and exp_lng is not None

    assignments = rep.get("assignments") or []
    tasks = rep.get("tasks") or []

    lines = [
        f"{W} *\u0426\u0435\u043d\u0442\u0440 \u0441\u043c\u0435\u043d\u044b #{shift_id}*",
        f"\u0414\u0430\u0442\u0430: {shift_date} {st_t}\u2013{en_t}",
        f"\u041b\u043e\u043a\u0430\u0446\u0438\u044f: {loc}",
    ]
    if include_pay:
        lines.append(f"\u0421\u0442\u0430\u0432\u043a\u0430: {rate} \u20bd/\u0447")
    lines.append("")

    for a in assignments:
        name = _worker_name(a)
        st = str(a[3] or "")
        gok = _geo_ok_val(a)
        esc = a[I_ESCALATION_1H] if len(a) > I_ESCALATION_1H else None

        light, reason = _traffic_light(
            now=now,
            dt_start=dt_start,
            status=st,
            esc_sent=esc,
            checkin_ts=a[5],
            geo_ok=gok,
            shift_has_fence=shift_has_fence,
        )

        lines.append(f"{light} *{name}* \u2014 {reason}")
        lines.append(f"   \u0441\u0442\u0430\u0442\u0443\u0441: `{st}` | {_geo_line(a, shift_has_fence)}")
        lines.append(f"   \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0435: {_fmt_ts(a[4])}")
        lines.append(f"   \u0447\u0435\u043a-\u0438\u043d: {_fmt_ts(a[5])} | \u0447\u0435\u043a-\u0430\u0443\u0442: {_fmt_ts(a[7])}")
        if include_pay and st == "checked_out":
            lines.append(
                f"   \u0447\u0430\u0441\u044b: {float(a[9] or 0):.1f} | \u0432\u044b\u043f\u043b\u0430\u0442\u0430: {float(a[10] or 0):.0f} \u20bd"
            )

        ev: list[tuple[object | None, str]] = []
        if len(a) > I_ASSIGNED_NOTIFY and a[I_ASSIGNED_NOTIFY]:
            ev.append((a[I_ASSIGNED_NOTIFY], "\u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043e \u043d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0438"))
        if len(a) > I_REMINDER_12H and a[I_REMINDER_12H]:
            ev.append((a[I_REMINDER_12H], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 ~12\u0447"))
        if len(a) > I_REMINDER_3H and a[I_REMINDER_3H]:
            ev.append((a[I_REMINDER_3H], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 ~3\u0447"))
        if len(a) > I_ESCALATION_1H and a[I_ESCALATION_1H]:
            ev.append((a[I_ESCALATION_1H], "\u044d\u0441\u043a\u0430\u043b\u0430\u0446\u0438\u044f: \u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b"))
        if len(a) > I_CHECKIN_30M and a[I_CHECKIN_30M]:
            ev.append((a[I_CHECKIN_30M], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0447\u0435\u043a-\u0438\u043d 30\u043c"))
        if len(a) > I_CHECKOUT_30M and a[I_CHECKOUT_30M]:
            ev.append((a[I_CHECKOUT_30M], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0447\u0435\u043a-\u0430\u0443\u0442 30\u043c"))
        if len(a) > I_FORGOT_CO and a[I_FORGOT_CO]:
            ev.append((a[I_FORGOT_CO], "\u0437\u0430\u0431\u044b\u043b\u0438 \u0447\u0435\u043a-\u0430\u0443\u0442"))
        if a[4]:
            ev.append((a[4], "\u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b \u0432\u044b\u0445\u043e\u0434"))
        if a[5]:
            ev.append((a[5], "\u0447\u0435\u043a-\u0438\u043d"))
        if len(a) > I_EXT_REQ_AT and a[I_EXT_REQ_AT]:
            ev.append((a[I_EXT_REQ_AT], "\u0437\u0430\u043f\u0440\u043e\u0441 \u043f\u0440\u043e\u0434\u043b\u0435\u043d\u0438\u044f"))
        if len(a) > I_EXT_RES_AT and a[I_EXT_RES_AT]:
            ev.append((a[I_EXT_RES_AT], "\u0440\u0435\u0448\u0435\u043d\u0438\u0435 \u043f\u043e \u043f\u0440\u043e\u0434\u043b\u0435\u043d\u0438\u044e"))
        if a[7]:
            ev.append((a[7], "\u0447\u0435\u043a-\u0430\u0443\u0442"))

        def _ts_key(x):
            t0 = _parse_ts(x[0])
            return t0 or datetime.min

        ev.sort(key=_ts_key)
        if ev:
            lines.append("   _\u0422\u0430\u0439\u043c\u043b\u0430\u0439\u043d:_")
            for ts, label in ev:
                lines.append(f"   \u2022 {_fmt_ts(ts)} \u2014 {label}")
        lines.append("")

    if tasks:
        lines.append("*\u0417\u0430\u0434\u0430\u0447\u0438 \u043f\u043e \u0441\u043c\u0435\u043d\u0435:*")
        for trow in tasks[:15]:
            tid = trow[0]
            title = trow[2]
            tst = trow[5]
            wn = trow[-1] if len(trow) > 6 else "\u2014"
            mark = G if str(tst) == "completed" else Y
            lines.append(f"   {mark} #{tid} {title} — {wn}")
    return "\n".join(lines)[:3900]
