"""Shift control center: traffic light + timeline (demo)."""
from __future__ import annotations

from datetime import datetime

from services.shift_notifier import _shift_start_end
from db import format_date_ru
from services.time_utils import now_local_naive

# Индексы в строке SELECT a.*, w.full_name, w.phone — порядок колонок как в init_db + миграции assignments.
I_ASSIGNED_NOTIFY = 11
I_REMINDER_12H = 12
I_REMINDER_12H_REPEAT = 13
I_REMINDER_3H = 14
I_ESCALATION_11H = 15
I_ESCALATION_1H = 16
I_CHECKIN_30M = 17
I_CHECKIN_15M = 18
I_CHECKOUT_30M = 19
I_FORGOT_CO = 20
I_EXT_REQ_AT = 26
I_EXT_RES_AT = 27
I_GEO_OK = 30
I_CONF_SHIFT_12H = 34
I_CONF_SHIFT_3H = 35
JOIN_MIN_LEN = 32

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
    val = row[I_GEO_OK]
    # На старых/рассинхронизированных схемах здесь может оказаться не int-флаг.
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        s = val.strip()
        if s in {"0", "1"}:
            return int(s)
    return None


def _geo_line(row: tuple, shift_has_fence: bool) -> str:
    ok = _geo_ok_val(row)
    if ok is None:
        return "\u0433\u0435\u043e: \u043d\u0435 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u0438\u0440\u0443\u0435\u0442\u0441\u044f" if not shift_has_fence else "\u0433\u0435\u043e: \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0435"
    if ok == 1:
        return "\u0433\u0435\u043e: \u0432 \u0440\u0430\u0434\u0438\u0443\u0441\u0435"
    if ok == 0:
        return "\u0433\u0435\u043e: \u0431\u044b\u043b\u0438 \u043f\u043e\u043f\u044b\u0442\u043a\u0438 \u0432\u043d\u0435 \u0440\u0430\u0434\u0438\u0443\u0441\u0430"
    return "\u0433\u0435\u043e: ?"


def _norm_geo_flag(geo_ok) -> int | None:
    if geo_ok is None:
        return None
    if isinstance(geo_ok, bool):
        return 1 if geo_ok else 0
    if isinstance(geo_ok, (int, float)):
        return int(geo_ok)
    if isinstance(geo_ok, str):
        s = geo_ok.strip()
        if s in {"0", "1"}:
            return int(s)
    return None


def _shift_geo_fields(shift: tuple) -> tuple[float | None, float | None]:
    if not shift or len(shift) < 12:
        return None, None

    def _to_float(v):
        try:
            return float(v) if v is not None and str(v).strip() != "" else None
        except Exception:
            return None

    # Новая схема: lat/lng/radius/created_at
    lat = _to_float(shift[8])
    lng = _to_float(shift[9])
    if lat is not None and lng is not None:
        return lat, lng

    # Старая мигрированная схема: created_at/lat/lng/radius
    lat = _to_float(shift[9])
    lng = _to_float(shift[10])
    if lat is not None and lng is not None:
        return lat, lng

    return None, None


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
    geo_flag = _norm_geo_flag(geo_ok)

    if st == "cancelled":
        return W, "\u043e\u0442\u043c\u0435\u043d\u0430 \u0434\u043b\u044f \u0438\u0441\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044f"

    if st == "checked_out":
        return G, "\u0441\u043c\u0435\u043d\u0430 \u0437\u0430\u043a\u0440\u044b\u0442\u0430"

    if st == "checked_in":
        ci = _parse_ts(checkin_ts)
        late_ci = bool(ci and ci > dt_start)
        if geo_flag == 0 and shift_has_fence:
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
    now = now or now_local_naive()
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

    exp_lat, exp_lng = _shift_geo_fields(shift)
    shift_has_fence = exp_lat is not None and exp_lng is not None

    assignments = rep.get("assignments") or []
    tasks = rep.get("tasks") or []

    lines = [
        f"{W} \u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043f\u043e \u0441\u043c\u0435\u043d\u0435 #{shift_id}",
        f"\u0414\u0430\u0442\u0430: {format_date_ru(shift_date)} {st_t}\u2013{en_t}",
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

        lines.append(f"{light} {name} \u2014 {reason}")
        lines.append(f"   \u0441\u0442\u0430\u0442\u0443\u0441: {st} | {_geo_line(a, shift_has_fence)}")
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
        if len(a) > I_REMINDER_12H_REPEAT and a[I_REMINDER_12H_REPEAT]:
            ev.append((a[I_REMINDER_12H_REPEAT], "\u043f\u043e\u0432\u0442\u043e\u0440 ~12\u0447 (\u043a\u0430\u0436\u0434\u044b\u0435 15\u043c)"))
        if len(a) > I_REMINDER_3H and a[I_REMINDER_3H]:
            ev.append((a[I_REMINDER_3H], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 ~3\u0447"))
        if len(a) > I_ESCALATION_11H and a[I_ESCALATION_11H]:
            ev.append((a[I_ESCALATION_11H], "\u044d\u0441\u043a\u0430\u043b\u0430\u0446\u0438\u044f 11\u0447 (\u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b)"))
        if len(a) > I_ESCALATION_1H and a[I_ESCALATION_1H]:
            ev.append((a[I_ESCALATION_1H], "\u044d\u0441\u043a\u0430\u043b\u0430\u0446\u0438\u044f: \u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b"))
        if len(a) > I_CHECKIN_30M and a[I_CHECKIN_30M]:
            ev.append((a[I_CHECKIN_30M], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0447\u0435\u043a-\u0438\u043d 30\u043c"))
        if len(a) > I_CHECKIN_15M and a[I_CHECKIN_15M]:
            ev.append((a[I_CHECKIN_15M], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0447\u0435\u043a-\u0438\u043d 15\u043c"))
        if len(a) > I_CHECKOUT_30M and a[I_CHECKOUT_30M]:
            ev.append((a[I_CHECKOUT_30M], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0447\u0435\u043a-\u0430\u0443\u0442 30\u043c"))
        if len(a) > I_FORGOT_CO and a[I_FORGOT_CO]:
            ev.append((a[I_FORGOT_CO], "\u0437\u0430\u0431\u044b\u043b\u0438 \u0447\u0435\u043a-\u0430\u0443\u0442"))
        if len(a) > I_CONF_SHIFT_12H and a[I_CONF_SHIFT_12H]:
            ev.append(
                (a[I_CONF_SHIFT_12H], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u043e \u0441\u043c\u0435\u043d\u0435 ~12\u0447 (\u0432\u044b\u0445\u043e\u0434 \u0443\u0436\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d)")
            )
        if len(a) > I_CONF_SHIFT_3H and a[I_CONF_SHIFT_3H]:
            ev.append(
                (a[I_CONF_SHIFT_3H], "\u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u043e \u0441\u043c\u0435\u043d\u0435 ~3\u0447 (\u0432\u044b\u0445\u043e\u0434 \u0443\u0436\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d)")
            )
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
            lines.append("   \u0422\u0430\u0439\u043c\u043b\u0430\u0439\u043d:")
            for ts, label in ev:
                lines.append(f"   \u2022 {_fmt_ts(ts)} \u2014 {label}")
        lines.append("")

    if tasks:
        lines.append("\u0417\u0430\u0434\u0430\u0447\u0438 \u043f\u043e \u0441\u043c\u0435\u043d\u0435:")
        for trow in tasks[:15]:
            tid = trow[0]
            title = trow[2]
            tst = trow[5]
            wn = trow[-1] if len(trow) > 6 else "\u2014"
            mark = G if str(tst) == "completed" else Y
            lines.append(f"   {mark} #{tid} {title} — {wn}")
    return "\n".join(lines)[:3900]
