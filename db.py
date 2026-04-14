# db.py
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

# База всегда рядом с кодом (не зависит от текущей директории при запуске)
DB_PATH = Path(__file__).resolve().parent / "promostaff_demo.db"


def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


def init_db():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workers (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            phone TEXT,
            profession TEXT,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            user_id INTEGER PRIMARY KEY,
            company_name TEXT,
            contact_name TEXT,
            phone TEXT,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            client_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            shift_date DATE NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            location TEXT,
            rate INTEGER DEFAULT 500,
            status TEXT DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER,
            worker_id INTEGER,
            status TEXT DEFAULT 'pending',
            confirmed_at TIMESTAMP,
            checkin_time TIMESTAMP,
            checkin_photo TEXT,
            checkout_time TIMESTAMP,
            checkout_photo TEXT,
            hours_worked REAL DEFAULT 0,
            payment REAL DEFAULT 0
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER,
            title TEXT NOT NULL,
            description TEXT,
            assigned_to INTEGER,
            status TEXT DEFAULT 'pending',
            completed_at TIMESTAMP,
            report_text TEXT,
            report_photo TEXT
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER,
            user_id INTEGER,
            user_name TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.commit()
    conn.close()
    print("✅ База данных инициализирована")


def normalize_shift_date(date_in: str) -> str:
    """ДД.ММ.ГГГГ или ГГГГ-ММ-ДД → ГГГГ-ММ-ДД для SQLite DATE."""
    s = (date_in or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    parts = s.split(".")
    if len(parts) == 3:
        d, m, y = parts[0].strip(), parts[1].strip(), parts[2].strip()
        if len(y) == 4 and d.isdigit() and m.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    raise ValueError("Дата: ДД.ММ.ГГГГ или ГГГГ-ММ-ДД")


def _parse_sqlite_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        s = value.replace(" ", "T", 1) if "T" not in value else value
        return datetime.fromisoformat(s)
    raise TypeError("ожидалась дата/строка из SQLite")


# ========== ИСПОЛНИТЕЛИ ==========
def save_worker(user_id: int, data: dict):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO workers (user_id, full_name, phone, profession)
        VALUES (?, ?, ?, ?)
    """,
        (user_id, data.get("full_name"), data.get("phone"), data.get("profession")),
    )
    conn.commit()
    conn.close()


def get_worker(user_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM workers WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_workers() -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, full_name, phone, profession FROM workers ORDER BY registered_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ========== ЗАКАЗЧИКИ ==========
def save_client(user_id: int, data: dict):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO clients (user_id, company_name, contact_name, phone)
        VALUES (?, ?, ?, ?)
    """,
        (user_id, data.get("company_name"), data.get("contact_name"), data.get("phone")),
    )
    conn.commit()
    conn.close()


def get_client(user_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clients WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def list_clients() -> list:
    """Список заказчиков для админки: (user_id, company_name, contact_name, phone)."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, company_name, contact_name, phone
        FROM clients ORDER BY registered_at DESC
    """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def delete_client_cascade(client_user_id: int) -> None:
    """Удалить заказчика и все его проекты / смены / назначения / задачи / чат по сменам."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM projects WHERE client_id = ?", (client_user_id,))
    project_ids = [r[0] for r in cur.fetchall()]
    for pid in project_ids:
        cur.execute("SELECT id FROM shifts WHERE project_id = ?", (pid,))
        shift_ids = [r[0] for r in cur.fetchall()]
        for sid in shift_ids:
            cur.execute("DELETE FROM chat_messages WHERE shift_id = ?", (sid,))
            cur.execute("DELETE FROM tasks WHERE shift_id = ?", (sid,))
            cur.execute("DELETE FROM assignments WHERE shift_id = ?", (sid,))
        cur.execute("DELETE FROM shifts WHERE project_id = ?", (pid,))
    cur.execute("DELETE FROM projects WHERE client_id = ?", (client_user_id,))
    cur.execute("DELETE FROM clients WHERE user_id = ?", (client_user_id,))
    conn.commit()
    conn.close()


# ========== ПРОЕКТЫ И СМЕНЫ ==========
def create_project(name: str, client_id: int) -> int:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO projects (name, client_id) VALUES (?, ?)", (name, client_id))
    project_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(project_id)


def list_projects_for_client(client_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name FROM projects WHERE client_id = ? ORDER BY id DESC",
        (client_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_projects_admin(limit: int = 30) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.name, p.client_id, c.company_name, c.contact_name
        FROM projects p
        LEFT JOIN clients c ON c.user_id = p.client_id
        ORDER BY p.created_at DESC
        LIMIT ?
    """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def create_shift(project_id: int, data: dict) -> int:
    date_iso = normalize_shift_date(data["date"])
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO shifts (project_id, shift_date, start_time, end_time, location, rate)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            project_id,
            date_iso,
            data["start_time"],
            data["end_time"],
            data["location"],
            data.get("rate", 500),
        ),
    )
    shift_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(shift_id)


def get_shifts_by_project(project_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM shifts WHERE project_id = ? ORDER BY shift_date", (project_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_shifts_for_client(client_user_id: int) -> list:
    """Смены по всем проектам заказчика."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.shift_date, s.start_time, s.end_time, s.location, p.name, s.status
        FROM shifts s
        JOIN projects p ON s.project_id = p.id
        WHERE p.client_id = ?
        ORDER BY s.shift_date DESC
    """,
        (client_user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_shifts_for_worker(worker_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.shift_date, s.start_time, s.end_time, s.location, a.status
        FROM assignments a
        JOIN shifts s ON a.shift_id = s.id
        WHERE a.worker_id = ?
        ORDER BY s.shift_date DESC
    """,
        (worker_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_shifts_admin(limit: int = 30) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.shift_date, s.start_time, s.end_time, p.name
        FROM shifts s
        JOIN projects p ON s.project_id = p.id
        ORDER BY s.shift_date DESC
        LIMIT ?
    """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_shift(shift_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM shifts WHERE id = ?", (shift_id,))
    row = cur.fetchone()
    conn.close()
    return row


def client_owns_shift(client_user_id: int, shift_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM shifts s
        JOIN projects p ON s.project_id = p.id
        WHERE s.id = ? AND p.client_id = ?
    """,
        (shift_id, client_user_id),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def assign_worker(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO assignments (shift_id, worker_id, status)
        VALUES (?, ?, 'pending')
    """,
        (shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def confirm_assignment(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE assignments SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP
        WHERE shift_id = ? AND worker_id = ?
    """,
        (shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def get_assignment(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM assignments WHERE shift_id = ? AND worker_id = ?",
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_shift_assignments(shift_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.*, w.full_name, w.phone FROM assignments a
        JOIN workers w ON a.worker_id = w.user_id
        WHERE a.shift_id = ?
    """,
        (shift_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def do_checkin(shift_id: int, worker_id: int, photo_url: str | None = None):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE assignments SET status = 'checked_in', checkin_time = CURRENT_TIMESTAMP, checkin_photo = ?
        WHERE shift_id = ? AND worker_id = ?
    """,
        (photo_url, shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def do_checkout(shift_id: int, worker_id: int, photo_url: str | None = None):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT checkin_time FROM assignments WHERE shift_id = ? AND worker_id = ?",
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    if row and row[0]:
        checkin_time = _parse_sqlite_ts(row[0])
        now = datetime.now()
        hours = (now - checkin_time).total_seconds() / 3600.0

        cur.execute("SELECT rate FROM shifts WHERE id = ?", (shift_id,))
        shift_row = cur.fetchone()
        rate = shift_row[0] if shift_row else 500

        payment = hours * float(rate)

        cur.execute(
            """
            UPDATE assignments SET status = 'checked_out', checkout_time = CURRENT_TIMESTAMP,
            checkout_photo = ?, hours_worked = ?, payment = ?
            WHERE shift_id = ? AND worker_id = ?
        """,
            (photo_url, hours, payment, shift_id, worker_id),
        )

    conn.commit()
    conn.close()


# ========== ЗАДАЧИ ==========
def create_task(shift_id: int, title: str, description: str, assigned_to: int | None = None) -> int:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tasks (shift_id, title, description, assigned_to)
        VALUES (?, ?, ?, ?)
    """,
        (shift_id, title, description, assigned_to),
    )
    task_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(task_id)


def get_worker_tasks(shift_id: int, worker_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM tasks WHERE shift_id = ? AND assigned_to = ?",
        (shift_id, worker_id),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_shifts_with_open_tasks_for_worker(worker_id: int) -> list:
    """Смены, где у исполнителя есть незавершённые назначенные задачи."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT s.id, s.shift_date, s.start_time, s.end_time, s.location
        FROM tasks t
        JOIN shifts s ON t.shift_id = s.id
        WHERE t.assigned_to = ? AND t.status != 'completed'
        ORDER BY s.shift_date DESC
    """,
        (worker_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def complete_task(task_id: int, report_text: str, report_photo: str | None = None):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tasks SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
        report_text = ?, report_photo = ?
        WHERE id = ?
    """,
        (report_text, report_photo, task_id),
    )
    conn.commit()
    conn.close()


def get_shift_tasks(shift_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE shift_id = ?", (shift_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


# ========== ЧАТ ==========
def save_chat_message(shift_id: int, user_id: int, user_name: str, message: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chat_messages (shift_id, user_id, user_name, message)
        VALUES (?, ?, ?, ?)
    """,
        (shift_id, user_id, user_name, message),
    )
    conn.commit()
    conn.close()


def get_chat_messages(shift_id: int, limit: int = 20) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_name, message, created_at FROM chat_messages
        WHERE shift_id = ? ORDER BY created_at DESC LIMIT ?
    """,
        (shift_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ========== ОТЧЁТЫ ==========
def get_shift_report(shift_id: int) -> dict:
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT * FROM shifts WHERE id = ?", (shift_id,))
    shift = cur.fetchone()

    cur.execute(
        """
        SELECT a.*, w.full_name FROM assignments a
        JOIN workers w ON a.worker_id = w.user_id
        WHERE a.shift_id = ?
    """,
        (shift_id,),
    )
    assignments = cur.fetchall()

    cur.execute(
        """
        SELECT t.*, w.full_name as worker_name FROM tasks t
        LEFT JOIN workers w ON t.assigned_to = w.user_id
        WHERE t.shift_id = ?
    """,
        (shift_id,),
    )
    tasks = cur.fetchall()

    conn.close()

    return {"shift": shift, "assignments": assignments, "tasks": tasks}
