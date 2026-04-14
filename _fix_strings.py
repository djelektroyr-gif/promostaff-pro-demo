# -*- coding: utf-8 -*-
import re
from pathlib import Path

path = Path(__file__).resolve().parent / "handlers" / "tasks.py"
t = path.read_text(encoding="utf-8")


def sub(pat: str, repl: str, n: int = 1) -> None:
    global t
    t2, c = re.subn(pat, repl, t, count=n, flags=re.DOTALL)
    if c != n:
        raise SystemExit(f"sub got {c}, need {n}, pat={pat!r}")
    t = t2


sub(
    r'f"Создано задач: \{len\(pairs\)\} — по одной каждому на смене\.\s*\n� \{title\}"',
    "f\"Создано задач: {len(pairs)} — по одной каждому на смене.\\n\\n�� {title}\"",
    1,
)
sub(
    r'f"Задача создана без исполнителя\.\s*\n\s*\n�� \{title\}"',
    "f\"Задача создана без исполнителя.\\� {title}\"",
    1,
)
sub(
    r'f"Создано задач: \{len\(pairs\)\}\.\s*\n� \{title\}"',
    "f\"Создано задач: {len(pairs)}.\\� {title}\"",
    1,
)
sub(
    r'f"�� Задача создана!\s*\n� \{title\}"',
    "f\"�� Задача создана!\\n\\n�� {title}\"",
    1,
)

t2, c = re.subn(
    r"    picked = list\(data\.get\(\"task_picked\"\) or \[\]\)\n    if not picked:",
    '    picked = list(dict.fromkeys(int(x) for x in (data.get("task_picked") or [])))\n    if not picked:',
    t,
    count=1,
)
if c != 1:
    raise SystemExit("pick line")
t = t2

path.write_text(t, encoding="utf-8")
print("ok")
