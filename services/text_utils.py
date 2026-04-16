# services/text_utils.py — экранирование для Telegram MarkdownV2
from __future__ import annotations

# Символы, которые в MarkdownV2 нужно экранировать обратным слэшем (см. документацию Telegram Bot API).
_MDV2_SPECIAL = frozenset("_*[]()~`>#+-=|{}.!\\")


def escape_markdown(text: str | int | float | None) -> str:
    """Экранирование пользовательского/динамического текста для parse_mode=MarkdownV2."""
    if text is None:
        return ""
    s = str(text)
    out: list[str] = []
    for ch in s:
        if ch in _MDV2_SPECIAL:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def safe_markdown(text: str | int | float | None) -> str:
    """То же, что escape_markdown; имя подчёркивает использование в небезопасных местах."""
    return escape_markdown(text)


def bold(text: str | int | float | None) -> str:
    """Жирный фрагмент MarkdownV2: *…* с экранированием содержимого."""
    return "*" + escape_markdown(text) + "*"
