# db.py
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
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
            status TEXT DEFAULT 'new',
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
            expected_lat REAL,
            expected_lng REAL,
            checkin_radius_m INTEGER DEFAULT 300,
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
    # Миграция assignments: служебные поля уведомлений/эскалаций/продлений.
    cur.execute("PRAGMA table_info(assignments)")
    assignment_cols = {r[1] for r in cur.fetchall()}
    if "assigned_notify_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN assigned_notify_sent_at TIMESTAMP")
    if "reminder_12h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN reminder_12h_sent_at TIMESTAMP")
    if "reminder_3h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN reminder_3h_sent_at TIMESTAMP")
    if "escalation_1h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN escalation_1h_sent_at TIMESTAMP")
    if "checkin_30m_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_30m_sent_at TIMESTAMP")
    if "checkout_30m_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkout_30m_sent_at TIMESTAMP")
    if "forgot_checkout_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN forgot_checkout_sent_at TIMESTAMP")
    if "late_checkin_notified_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN late_checkin_notified_at TIMESTAMP")
    if "extension_request_minutes" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_request_minutes INTEGER")
    if "extension_request_status" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_request_status TEXT")
    if "extension_requested_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_requested_at TIMESTAMP")
    if "extension_resolved_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_resolved_at TIMESTAMP")

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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id INTEGER,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at ON admin_logs(created_at DESC)")

    # Миграция: у старых БД может не быть колонки workers.status
    cur.execute("PRAGMA table_info(workers)")
    worker_cols = {r[1] for r in cur.fetchall()}
    if "status" not in worker_cols:
        cur.execute("ALTER TABLE workers ADD COLUMN status TEXT DEFAULT 'new'")
    cur.execute("UPDATE workers SET status = 'new' WHERE status IS NULL OR TRIM(status) = ''")

    # Миграция: координаты площадки для проверки чек-ина.
    cur.execute("PRAGMA table_info(shifts)")
    shift_cols = {r[1] for r in cur.fetchall()}
    if "expected_lat" not in shift_cols:
        cur.execute("ALTER TABLE shifts ADD COLUMN expected_lat REAL")
    if "expected_lng" not in shift_cols:
        cur.execute("ALTER TABLE shifts ADD COLUMN expected_lng REAL")
    if "checkin_radius_m" not in shift_cols:
        cur.execute("ALTER TABLE shifts ADD COLUMN checkin_radius_m INTEGER DEFAULT 300")

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
        INSERT INTO workers (user_id, full_name, phone, profession, status)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            full_name = excluded.full_name,
            phone = excluded.phone,
            profession = excluded.profession
    """,
        (
            user_id,
            data.get("full_name"),
            data.get("phone"),
            data.get("profession"),
            data.get("status") or "new",
        ),
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


def get_workers(status: str | None = None) -> list:
    conn = db_connect()
    cur = conn.cursor()
    if status and status != "all":
        cur.execute(
            """
            SELECT user_id, full_name, phone, profession, status
            FROM workers
            WHERE status = ?
            ORDER BY registered_at DESC
            """,
            (status,),
        )
    else:
        cur.execute(
            """
            SELECT user_id, full_name, phone, profession, status
            FROM workers
            ORDER BY registered_at DESC
            """
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_workers_assignable() -> list:
    """
    Исполнители, доступные для назначения:
    все, кроме явно отклонённых (rejected).
    Приоритет в выдаче: approved -> reviewed -> new -> прочие.
    """
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, full_name, phone, profession, status
        FROM workers
        WHERE COALESCE(status, 'new') != 'rejected'
        ORDER BY
            CASE COALESCE(status, 'new')
                WHEN 'approved' THEN 0
                WHEN 'reviewed' THEN 1
                WHEN 'new' THEN 2
                ELSE 3
            END,
            registered_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_worker_status_counts() -> dict:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT status, COUNT(*)
        FROM workers
        GROUP BY status
        """
    )
    rows = cur.fetchall()
    conn.close()
    out = {"new": 0, "reviewed": 0, "approved": 0, "rejected": 0}
    for status, cnt in rows:
        out[str(status or "new")] = int(cnt or 0)
    out["all"] = sum(out.values())
    return out


def set_worker_status(worker_id: int, status: str) -> bool:
    if status not in {"new", "reviewed", "approved", "rejected"}:
        return False
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("UPDATE workers SET status = ? WHERE user_id = ?", (status, worker_id))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def get_worker_assignment_stats(worker_id: int) -> dict:
    """Короткая статистика по назначениям исполнителя для админки."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM assignments WHERE worker_id = ?", (worker_id,))
    assignments_total = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM tasks
        WHERE assigned_to = ? AND status != 'completed'
        """,
        (worker_id,),
    )
    open_tasks = int(cur.fetchone()[0] or 0)
    conn.close()
    return {"assignments_total": assignments_total, "open_tasks": open_tasks}


def delete_worker_safe(worker_id: int) -> dict:
    """
    Безопасно удалить исполнителя:
    - удаляем назначения из assignments,
    - отвязываем назначенные задачи (assigned_to = NULL),
    - удаляем из workers.
    """
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM workers WHERE user_id = ?", (worker_id,))
    exists = cur.fetchone() is not None
    if not exists:
        conn.close()
        return {"deleted": False, "assignments_deleted": 0, "tasks_unassigned": 0}

    cur.execute("SELECT COUNT(*) FROM assignments WHERE worker_id = ?", (worker_id,))
    assignments_deleted = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*) FROM tasks
        WHERE assigned_to = ? AND status != 'completed'
        """,
        (worker_id,),
    )
    tasks_unassigned = int(cur.fetchone()[0] or 0)

    cur.execute("DELETE FROM assignments WHERE worker_id = ?", (worker_id,))
    cur.execute("UPDATE tasks SET assigned_to = NULL WHERE assigned_to = ?", (worker_id,))
    cur.execute("DELETE FROM workers WHERE user_id = ?", (worker_id,))

    conn.commit()
    conn.close()
    return {
        "deleted": True,
        "assignments_deleted": assignments_deleted,
        "tasks_unassigned": tasks_unassigned,
    }


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


def delete_project_cascade(project_id: int) -> dict:
    """Удалить проект и все связанные смены/назначения/задачи/чат."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM projects WHERE id = ?", (project_id,))
    if not cur.fetchone():
        conn.close()
        return {"deleted": False, "shifts": 0, "assignments": 0, "tasks": 0, "chat_messages": 0}
    cur.execute("SELECT id FROM shifts WHERE project_id = ?", (project_id,))
    shift_ids = [int(r[0]) for r in cur.fetchall()]
    shifts_count = len(shift_ids)
    assignments_count = 0
    tasks_count = 0
    chat_count = 0
    for sid in shift_ids:
        cur.execute("SELECT COUNT(*) FROM assignments WHERE shift_id = ?", (sid,))
        assignments_count += int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM tasks WHERE shift_id = ?", (sid,))
        tasks_count += int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM chat_messages WHERE shift_id = ?", (sid,))
        chat_count += int(cur.fetchone()[0] or 0)
        cur.execute("DELETE FROM assignments WHERE shift_id = ?", (sid,))
        cur.execute("DELETE FROM tasks WHERE shift_id = ?", (sid,))
        cur.execute("DELETE FROM chat_messages WHERE shift_id = ?", (sid,))
    cur.execute("DELETE FROM shifts WHERE project_id = ?", (project_id,))
    cur.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()
    return {
        "deleted": True,
        "shifts": shifts_count,
        "assignments": assignments_count,
        "tasks": tasks_count,
        "chat_messages": chat_count,
    }


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
        INSERT INTO shifts (
            project_id, shift_date, start_time, end_time, location, rate,
            expected_lat, expected_lng, checkin_radius_m
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            project_id,
            date_iso,
            data["start_time"],
            data["end_time"],
            data["location"],
            data.get("rate", 500),
            data.get("expected_lat"),
            data.get("expected_lng"),
            data.get("checkin_radius_m", 300),
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


def list_open_shifts_admin(limit: int = 30) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.shift_date, s.start_time, s.end_time, p.name, s.status
        FROM shifts s
        JOIN projects p ON s.project_id = p.id
        WHERE s.status = 'open'
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


def close_shift_safe(shift_id: int) -> dict:
    """Закрывает смену без удаления данных."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT status FROM shifts WHERE id = ?", (shift_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"closed": False, "reason": "not_found"}
    if row[0] == "closed":
        conn.close()
        return {"closed": True, "reason": "already_closed"}
    cur.execute("UPDATE shifts SET status = 'closed' WHERE id = ?", (shift_id,))
    cur.execute(
        """
        UPDATE assignments
        SET status = CASE
            WHEN status IN ('checked_out', 'checked_in') THEN status
            ELSE 'cancelled'
        END
        WHERE shift_id = ?
        """,
        (shift_id,),
    )
    conn.commit()
    conn.close()
    return {"closed": True, "reason": "ok"}


def delete_shift_cascade(shift_id: int) -> dict:
    """Удаляет смену и связанные назначения/задачи/чат."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM shifts WHERE id = ?", (shift_id,))
    if not cur.fetchone():
        conn.close()
        return {"deleted": False, "assignments": 0, "tasks": 0, "chat_messages": 0}
    cur.execute("SELECT COUNT(*) FROM assignments WHERE shift_id = ?", (shift_id,))
    ac = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM tasks WHERE shift_id = ?", (shift_id,))
    tc = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM chat_messages WHERE shift_id = ?", (shift_id,))
    cc = int(cur.fetchone()[0] or 0)
    cur.execute("DELETE FROM assignments WHERE shift_id = ?", (shift_id,))
    cur.execute("DELETE FROM tasks WHERE shift_id = ?", (shift_id,))
    cur.execute("DELETE FROM chat_messages WHERE shift_id = ?", (shift_id,))
    cur.execute("DELETE FROM shifts WHERE id = ?", (shift_id,))
    conn.commit()
    conn.close()
    return {"deleted": True, "assignments": ac, "tasks": tc, "chat_messages": cc}


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


def get_assignment_status(shift_id: int, worker_id: int) -> str | None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT status FROM assignments WHERE shift_id = ? AND worker_id = ?",
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


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


def get_shift_with_owner(shift_id: int):
    """Данные смены + заказчик проекта."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.shift_date, s.start_time, s.end_time, s.location, s.rate, s.status,
               p.client_id, p.name
        FROM shifts s
        JOIN projects p ON s.project_id = p.id
        WHERE s.id = ?
        """,
        (shift_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_assignments_for_scheduler() -> list:
    """Срез назначений для фоновых уведомлений."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            a.id, a.shift_id, a.worker_id, a.status,
            a.assigned_notify_sent_at, a.reminder_12h_sent_at, a.reminder_3h_sent_at,
            a.escalation_1h_sent_at, a.checkin_30m_sent_at, a.checkout_30m_sent_at,
            a.forgot_checkout_sent_at, a.late_checkin_notified_at,
            a.extension_request_minutes, a.extension_request_status,
            s.shift_date, s.start_time, s.end_time, s.location, s.status,
            p.client_id,
            w.full_name
        FROM assignments a
        JOIN shifts s ON s.id = a.shift_id
        JOIN projects p ON p.id = s.project_id
        LEFT JOIN workers w ON w.user_id = a.worker_id
        WHERE s.status IN ('open', 'in_progress')
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_assignment_event(assignment_id: int, field: str) -> None:
    allowed = {
        "assigned_notify_sent_at",
        "reminder_12h_sent_at",
        "reminder_3h_sent_at",
        "escalation_1h_sent_at",
        "checkin_30m_sent_at",
        "checkout_30m_sent_at",
        "forgot_checkout_sent_at",
        "late_checkin_notified_at",
    }
    if field not in allowed:
        return
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(f"UPDATE assignments SET {field} = CURRENT_TIMESTAMP WHERE id = ?", (assignment_id,))
    conn.commit()
    conn.close()


def mark_assignment_event_by_shift_worker(shift_id: int, worker_id: int, field: str) -> None:
    allowed = {
        "assigned_notify_sent_at",
        "reminder_12h_sent_at",
        "reminder_3h_sent_at",
        "escalation_1h_sent_at",
        "checkin_30m_sent_at",
        "checkout_30m_sent_at",
        "forgot_checkout_sent_at",
        "late_checkin_notified_at",
    }
    if field not in allowed:
        return
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE assignments SET {field} = CURRENT_TIMESTAMP WHERE shift_id = ? AND worker_id = ?",
        (shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def set_extension_request(shift_id: int, worker_id: int, minutes: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE assignments
        SET extension_request_minutes = ?, extension_request_status = 'pending',
            extension_requested_at = CURRENT_TIMESTAMP, extension_resolved_at = NULL
        WHERE shift_id = ? AND worker_id = ? AND status = 'checked_in'
        """,
        (minutes, shift_id, worker_id),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def get_pending_extension(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT extension_request_minutes, extension_request_status
        FROM assignments
        WHERE shift_id = ? AND worker_id = ?
        """,
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


def resolve_extension_request(shift_id: int, worker_id: int, approved: bool) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    status = "approved" if approved else "rejected"
    cur.execute(
        """
        UPDATE assignments
        SET extension_request_status = ?, extension_resolved_at = CURRENT_TIMESTAMP
        WHERE shift_id = ? AND worker_id = ? AND extension_request_status = 'pending'
        """,
        (status, shift_id, worker_id),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def extend_shift_end_time(shift_id: int, minutes: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT end_time FROM shifts WHERE id = ?", (shift_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False
    try:
        t = datetime.strptime(str(row[0]), "%H:%M")
    except Exception:
        conn.close()
        return False
    t2 = t + timedelta(minutes=minutes)
    cur.execute("UPDATE shifts SET end_time = ? WHERE id = ?", (t2.strftime("%H:%M"), shift_id))
    conn.commit()
    conn.close()
    return True


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


def get_task(task_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    row = cur.fetchone()
    conn.close()
    return row


def list_tasks_for_client(client_user_id: int, limit: int = 200) -> list:
    """
    Задачи по всем сменам заказчика.
    Возвращает: task_id, title, status, shift_id, shift_date, start_time, end_time, worker_name
    """
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            t.id,
            t.title,
            t.status,
            s.id,
            s.shift_date,
            s.start_time,
            s.end_time,
            COALESCE(w.full_name, 'Не назначен')
        FROM tasks t
        JOIN shifts s ON s.id = t.shift_id
        JOIN projects p ON p.id = s.project_id
        LEFT JOIN workers w ON w.user_id = t.assigned_to
        WHERE p.client_id = ?
        ORDER BY s.shift_date DESC, t.id DESC
        LIMIT ?
        """,
        (client_user_id, limit),
    )
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


def get_admin_metrics() -> dict:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM workers")
    workers = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM clients")
    clients = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM projects")
    projects = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM shifts WHERE status = 'open'")
    open_shifts = int(cur.fetchone()[0] or 0)
    conn.close()
    return {
        "workers": workers,
        "clients": clients,
        "projects": projects,
        "open_shifts": open_shifts,
    }


def log_admin_action(admin_user_id: int, action: str, entity_type: str, entity_id: int | None, details: str = "") -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO admin_logs (admin_user_id, action, entity_type, entity_id, details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (admin_user_id, action, entity_type, entity_id, details[:500]),
    )
    conn.commit()
    conn.close()


def list_admin_logs(limit: int = 25) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT admin_user_id, action, entity_type, entity_id, details, created_at
        FROM admin_logs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def seed_demo_data() -> dict:
    """
    Генератор тестовых данных для e2e:
    5 исполнителей, 2 заказчика, 2 проекта, 4 смены, назначения и 4 задачи.
    """
    conn = db_connect()
    cur = conn.cursor()
    base_w = 900000
    base_c = 800000

    workers = [
        (base_w + 1, "Иван Петров", "+79001111111", "Хелпер", "approved"),
        (base_w + 2, "Мария Смирнова", "+79002222222", "Промоутер", "reviewed"),
        (base_w + 3, "Олег Волков", "+79003333333", "Грузчик", "new"),
        (base_w + 4, "Анна Кузнецова", "+79004444444", "Хостес", "approved"),
        (base_w + 5, "Дмитрий Соколов", "+79005555555", "Парковщик", "rejected"),
    ]
    for row in workers:
        cur.execute(
            """
            INSERT INTO workers (user_id, full_name, phone, profession, status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name=excluded.full_name,
                phone=excluded.phone,
                profession=excluded.profession,
                status=excluded.status
            """,
            row,
        )

    clients = [
        (base_c + 1, "ООО Ивент Лаб", "Алексей", "+79991111111"),
        (base_c + 2, "ИП Романов", "Екатерина", "+79992222222"),
    ]
    for row in clients:
        cur.execute(
            """
            INSERT INTO clients (user_id, company_name, contact_name, phone)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                company_name=excluded.company_name,
                contact_name=excluded.contact_name,
                phone=excluded.phone
            """,
            row,
        )

    cur.execute("INSERT INTO projects (name, client_id) VALUES (?, ?)", ("Выставка PRO Demo", base_c + 1))
    p1 = int(cur.lastrowid)
    cur.execute("INSERT INTO projects (name, client_id) VALUES (?, ?)", ("Промо-акция Весна", base_c + 2))
    p2 = int(cur.lastrowid)

    today = datetime.now().date()
    shifts = [
        (p1, str(today + timedelta(days=1)), "10:00", "18:00", "Крокус Экспо", 500, "open"),
        (p1, str(today + timedelta(days=2)), "09:00", "17:00", "ВДНХ", 550, "open"),
        (p2, str(today + timedelta(days=1)), "12:00", "20:00", "ТЦ Европейский", 600, "open"),
        (p2, str(today + timedelta(days=3)), "11:00", "19:00", "Арбат, 1", 520, "open"),
    ]
    shift_ids: list[int] = []
    for s in shifts:
        cur.execute(
            """
            INSERT INTO shifts (project_id, shift_date, start_time, end_time, location, rate, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            s,
        )
        shift_ids.append(int(cur.lastrowid))

    assignments = [
        (shift_ids[0], base_w + 1, "pending"),
        (shift_ids[0], base_w + 4, "confirmed"),
        (shift_ids[1], base_w + 1, "pending"),
        (shift_ids[2], base_w + 4, "pending"),
    ]
    for a in assignments:
        cur.execute(
            """
            INSERT OR IGNORE INTO assignments (shift_id, worker_id, status)
            VALUES (?, ?, ?)
            """,
            a,
        )

    tasks = [
        (shift_ids[0], "Подготовить стойку регистрации", "Прибыть за 30 минут", base_w + 1),
        (shift_ids[0], "Раздача бейджей", "Работа на входе", base_w + 4),
        (shift_ids[2], "Раздача листовок", "Точка у входа", base_w + 4),
        (shift_ids[1], "Сбор коробов", "После завершения смены", base_w + 1),
    ]
    for t in tasks:
        cur.execute(
            """
            INSERT INTO tasks (shift_id, title, description, assigned_to)
            VALUES (?, ?, ?, ?)
            """,
            t,
        )

    conn.commit()
    conn.close()
    return {
        "workers": len(workers),
        "clients": len(clients),
        "projects": 2,
        "shifts": len(shifts),
        "assignments": len(assignments),
        "tasks": len(tasks),
    }
