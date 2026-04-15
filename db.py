# db.py
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import re
from urllib.parse import urlparse

import psycopg2

from config import DATABASE_URL
from config import BILLING_ROUNDING_MODE, LATE_CHECKIN_PENALTY_FIRST_30M_HOURS
from services.time_utils import now_local_naive, shift_start_end_local_naive

# База всегда рядом с кодом (не зависит от текущей директории при запуске)
DB_PATH = Path(__file__).resolve().parent / "promostaff_demo.db"
USE_POSTGRES = bool(DATABASE_URL)


class _PgCursorCompat:
    def __init__(self, cur):
        self._cur = cur

    @property
    def lastrowid(self):
        row = self._cur.fetchone()
        return int(row[0]) if row else None

    @property
    def rowcount(self):
        return self._cur.rowcount

    def _convert_sql(self, sql: str) -> str:
        s = sql
        s = s.replace("?", "%s")
        s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        s = s.replace("INSERT OR IGNORE INTO assignments", "INSERT INTO assignments")
        s = s.replace(
            "INSERT OR REPLACE INTO clients (user_id, company_name, contact_name, phone)\n        VALUES (%s, %s, %s, %s)",
            """
        INSERT INTO clients (user_id, company_name, contact_name, phone)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            contact_name = EXCLUDED.contact_name,
            phone = EXCLUDED.phone
            """.strip(),
        )
        return s

    def execute(self, sql, params=None):
        raw = (sql or "").strip().lower()
        if raw.startswith("pragma table_info("):
            table = re.sub(r"^pragma table_info\((.+)\)$", r"\1", raw)
            table = table.strip().strip("'").strip('"')
            self._cur.execute(
                """
                SELECT
                    ordinal_position - 1 AS cid,
                    column_name,
                    data_type,
                    CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                    column_default,
                    0 AS pk
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table,),
            )
            return
        self._cur.execute(self._convert_sql(sql), params or ())

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __getattr__(self, item):
        return getattr(self._cur, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _PgConnCompat:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _PgCursorCompat(self._conn.cursor())

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def __getattr__(self, item):
        return getattr(self._conn, item)


def db_connect():
    if USE_POSTGRES:
        return _PgConnCompat(psycopg2.connect(DATABASE_URL))
    return sqlite3.connect(str(DB_PATH))


_PG_ID_COLUMNS = (
    ("workers", "user_id"),
    ("clients", "user_id"),
    ("projects", "client_id"),
    ("assignments", "worker_id"),
    ("tasks", "assigned_to"),
    ("chat_messages", "user_id"),
    ("project_chat_messages", "user_id"),
    ("admin_logs", "admin_user_id"),
    ("admin_logs", "entity_id"),
)


def _pg_widen_telegram_id_columns(cur) -> None:
    """INT4 → BIGINT для Telegram ID и связанных полей (без ошибки, если уже BIGINT)."""
    for table, col in _PG_ID_COLUMNS:
        cur.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            """,
            (table, col),
        )
        row = cur.fetchone()
        if not row:
            continue
        dt = (row[0] or "").lower()
        if dt == "bigint":
            continue
        if dt not in ("integer", "smallint"):
            continue
        cur.execute(
            f"ALTER TABLE {table} ALTER COLUMN {col} TYPE BIGINT USING {col}::bigint"
        )


def _safe_db_url_summary() -> str:
    if not DATABASE_URL:
        return "(нет DATABASE_URL)"
    try:
        u = urlparse(DATABASE_URL)
        host = u.hostname or "?"
        port = f":{u.port}" if u.port else ""
        db = (u.path or "/").strip("/") or "?"
        return f"{u.scheme}://{host}{port}/{db}"
    except Exception:
        return "(не удалось разобрать DATABASE_URL)"


def get_db_status_report() -> str:
    """
    Текст для диагностики (без секретов): бэкенд, доступность, типы id-колонок в Postgres.
    """
    lines = [
        f"Бэкенд: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}",
        f"DSN (без пароля): {_safe_db_url_summary()}",
    ]
    if not USE_POSTGRES:
        lines.append(f"Файл SQLite: {DB_PATH}")
        try:
            conn = db_connect()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            n = int(cur.fetchone()[0])
            conn.close()
            lines.append(f"SQLite: подключение OK, таблиц: {n}")
        except Exception as e:
            lines.append(f"SQLite: ошибка — {e}")
        return "\n".join(lines)

    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute("SELECT current_database(), version()")
        dbname, ver = cur.fetchone()
        lines.append(f"PostgreSQL: подключение OK")
        lines.append(f"current_database: {dbname}")
        lines.append(f"version: {(ver or '')[:80]}...")
        for table, col in _PG_ID_COLUMNS:
            cur.execute(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
                """,
                (table, col),
            )
            r = cur.fetchone()
            lines.append(f"  {table}.{col}: {r[0] if r else '(нет колонки)'}")
        conn.close()
    except Exception as e:
        lines.append(f"PostgreSQL: ошибка — {type(e).__name__}: {e}")
    return "\n".join(lines)


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
    if "reminder_12h_repeat_last_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN reminder_12h_repeat_last_at TIMESTAMP")
    if "reminder_3h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN reminder_3h_sent_at TIMESTAMP")
    if "escalation_11h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN escalation_11h_sent_at TIMESTAMP")
    if "escalation_1h_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN escalation_1h_sent_at TIMESTAMP")
    if "checkin_30m_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_30m_sent_at TIMESTAMP")
    if "checkin_15m_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_15m_sent_at TIMESTAMP")
    if "checkout_30m_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkout_30m_sent_at TIMESTAMP")
    if "forgot_checkout_sent_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN forgot_checkout_sent_at TIMESTAMP")
    if "late_checkin_notified_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN late_checkin_notified_at TIMESTAMP")
    if "no_confirm_flagged_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN no_confirm_flagged_at TIMESTAMP")
    if "no_checkin_start_notified_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN no_checkin_start_notified_at TIMESTAMP")
    if "extension_request_minutes" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_request_minutes INTEGER")
    if "extension_request_status" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_request_status TEXT")
    if "extension_requested_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_requested_at TIMESTAMP")
    if "extension_resolved_at" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN extension_resolved_at TIMESTAMP")
    if "checkin_lat" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_lat REAL")
    if "checkin_lng" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_lng REAL")
    if "checkin_geo_ok" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN checkin_geo_ok INTEGER")
    if "billed_hours" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN billed_hours REAL DEFAULT 0")
    if "penalty_hours" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN penalty_hours REAL DEFAULT 0")
    if "billing_rounding_mode" not in assignment_cols:
        cur.execute("ALTER TABLE assignments ADD COLUMN billing_rounding_mode TEXT")
    if "confirmed_shift_12h_reminder_sent_at" not in assignment_cols:
        cur.execute(
            "ALTER TABLE assignments ADD COLUMN confirmed_shift_12h_reminder_sent_at TIMESTAMP"
        )
    if "confirmed_shift_3h_reminder_sent_at" not in assignment_cols:
        cur.execute(
            "ALTER TABLE assignments ADD COLUMN confirmed_shift_3h_reminder_sent_at TIMESTAMP"
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
            report_photo TEXT,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        CREATE TABLE IF NOT EXISTS project_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_project_chat_project ON project_chat_messages(project_id, created_at DESC)"
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            context TEXT,
            message TEXT,
            error TEXT,
            attempts INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_locks (
            lock_name TEXT PRIMARY KEY,
            owner_id TEXT,
            expires_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shift_replacements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER NOT NULL,
            old_worker_id INTEGER NOT NULL,
            new_worker_id INTEGER NOT NULL,
            actor_user_id INTEGER NOT NULL,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS assignment_breaks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER NOT NULL,
            worker_id INTEGER NOT NULL,
            break_type TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ended_at TIMESTAMP,
            note TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS overdue_task_pings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id INTEGER NOT NULL,
            worker_id INTEGER NOT NULL,
            ping_sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            escalated_at TIMESTAMP
        )
        """
    )

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

    cur.execute("PRAGMA table_info(tasks)")
    task_cols = {r[1] for r in cur.fetchall()}
    if "assigned_at" not in task_cols:
        cur.execute("ALTER TABLE tasks ADD COLUMN assigned_at TIMESTAMP")
    cur.execute("UPDATE tasks SET assigned_at = COALESCE(assigned_at, CURRENT_TIMESTAMP)")
    if "client_rating" not in task_cols:
        cur.execute("ALTER TABLE tasks ADD COLUMN client_rating INTEGER")
    if "client_rated_at" not in task_cols:
        cur.execute("ALTER TABLE tasks ADD COLUMN client_rated_at TIMESTAMP")

    # PostgreSQL: Telegram ID не помещаются в INTEGER — переводим в BIGINT (идемпотентно).
    if USE_POSTGRES:
        _pg_widen_telegram_id_columns(cur)

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


def format_date_ru(iso_or_any) -> str:
    """ГГГГ-ММ-ДД (или date) → ДД.ММ.ГГГГ для показа пользователю."""
    s = str(iso_or_any or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y, m, d = s[0:4], s[5:7], s[8:10]
        return f"{d}.{m}.{y}"
    return s


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
        INSERT INTO clients (user_id, company_name, contact_name, phone)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            company_name = excluded.company_name,
            contact_name = excluded.contact_name,
            phone = excluded.phone
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
    if USE_POSTGRES:
        cur.execute("INSERT INTO projects (name, client_id) VALUES (%s, %s) RETURNING id", (name, client_id))
        project_id = int(cur.fetchone()[0])
    else:
        cur.execute("INSERT INTO projects (name, client_id) VALUES (?, ?)", (name, client_id))
        project_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return project_id


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


def get_project(project_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name, client_id FROM projects WHERE id = ?", (project_id,))
    row = cur.fetchone()
    conn.close()
    return row


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
    if USE_POSTGRES:
        cur.execute(
            """
        INSERT INTO shifts (
            project_id, shift_date, start_time, end_time, location, rate,
            expected_lat, expected_lng, checkin_radius_m
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
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
        shift_id = int(cur.fetchone()[0])
    else:
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
        shift_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return shift_id


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


def client_owns_project(client_user_id: int, project_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM projects WHERE id = ? AND client_id = ?",
        (project_id, client_user_id),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def worker_assigned_to_project(worker_id: int, project_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM assignments a
        JOIN shifts s ON s.id = a.shift_id
        WHERE a.worker_id = ? AND s.project_id = ?
        LIMIT 1
        """,
        (worker_id, project_id),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def assign_worker(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO assignments (shift_id, worker_id, status)
        SELECT ?, ?, 'pending'
        WHERE NOT EXISTS (
            SELECT 1 FROM assignments WHERE shift_id = ? AND worker_id = ?
        )
    """,
        (shift_id, worker_id, shift_id, worker_id),
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


def replace_assignment_worker(shift_id: int, old_worker_id: int, new_worker_id: int) -> dict:
    """
    Без потери данных:
    - старого исполнителя не удаляем, а переводим в cancelled (если не начал смену);
    - нового добавляем в assignments в pending.
    """
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT status FROM assignments WHERE shift_id = ? AND worker_id = ?",
        (shift_id, old_worker_id),
    )
    old_row = cur.fetchone()
    if not old_row:
        conn.close()
        return {"ok": False, "reason": "old_not_found"}
    old_status = str(old_row[0] or "")
    if old_status in ("checked_in", "checked_out"):
        conn.close()
        return {"ok": False, "reason": "old_already_started"}
    cur.execute(
        "SELECT 1 FROM assignments WHERE shift_id = ? AND worker_id = ?",
        (shift_id, new_worker_id),
    )
    if cur.fetchone():
        conn.close()
        return {"ok": False, "reason": "new_already_assigned"}
    cur.execute(
        """
        UPDATE assignments
        SET status = 'cancelled'
        WHERE shift_id = ? AND worker_id = ? AND status NOT IN ('checked_in', 'checked_out')
        """,
        (shift_id, old_worker_id),
    )
    cur.execute(
        "INSERT INTO assignments (shift_id, worker_id, status) VALUES (?, ?, 'pending')",
        (shift_id, new_worker_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "reason": "replaced"}


def list_unconfirmed_assignments(shift_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.id, a.worker_id, a.status, a.confirmed_at, w.full_name
        FROM assignments a
        LEFT JOIN workers w ON w.user_id = a.worker_id
        WHERE a.shift_id = ? AND a.status IN ('pending')
        ORDER BY a.id
        """,
        (shift_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def start_assignment_break(shift_id: int, worker_id: int, break_type: str, note: str = "") -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM assignment_breaks
        WHERE shift_id = ? AND worker_id = ? AND ended_at IS NULL
        LIMIT 1
        """,
        (shift_id, worker_id),
    )
    if cur.fetchone():
        conn.close()
        return False
    cur.execute(
        """
        INSERT INTO assignment_breaks (shift_id, worker_id, break_type, note)
        VALUES (?, ?, ?, ?)
        """,
        (shift_id, worker_id, break_type, (note or "")[:500]),
    )
    conn.commit()
    conn.close()
    return True


def stop_assignment_break(shift_id: int, worker_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE assignment_breaks
        SET ended_at = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id FROM assignment_breaks
            WHERE shift_id = ? AND worker_id = ? AND ended_at IS NULL
            ORDER BY started_at DESC
            LIMIT 1
        )
        """,
        (shift_id, worker_id),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def get_active_break(shift_id: int, worker_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, break_type, started_at, note
        FROM assignment_breaks
        WHERE shift_id = ? AND worker_id = ? AND ended_at IS NULL
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_shift_breaks(shift_id: int) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT b.id, b.shift_id, b.worker_id, b.break_type, b.started_at, b.ended_at, b.note, w.full_name
        FROM assignment_breaks b
        LEFT JOIN workers w ON w.user_id = b.worker_id
        WHERE b.shift_id = ?
        ORDER BY b.started_at
        """,
        (shift_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def record_overdue_task_ping(shift_id: int, worker_id: int) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO overdue_task_pings (shift_id, worker_id)
        VALUES (?, ?)
        """,
        (shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def list_due_overdue_task_escalations(wait_minutes: int = 30) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cutoff = now_local_naive() - timedelta(minutes=int(wait_minutes))
    cur.execute(
        """
        SELECT
            p.id,
            p.shift_id,
            p.worker_id,
            p.ping_sent_at,
            pr.client_id,
            COALESCE(w.full_name, CAST(p.worker_id AS TEXT))
        FROM overdue_task_pings p
        JOIN shifts s ON s.id = p.shift_id
        JOIN projects pr ON pr.id = s.project_id
        LEFT JOIN workers w ON w.user_id = p.worker_id
        WHERE p.escalated_at IS NULL
          AND p.ping_sent_at <= ?
        ORDER BY p.id
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_overdue_task_escalated(ping_id: int) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE overdue_task_pings SET escalated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (ping_id,),
    )
    conn.commit()
    conn.close()


def has_open_tasks_for_worker_on_shift(shift_id: int, worker_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM tasks
        WHERE shift_id = ? AND assigned_to = ? AND status != 'completed'
        LIMIT 1
        """,
        (shift_id, worker_id),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def list_open_task_titles_for_worker_on_shift(shift_id: int, worker_id: int, limit: int = 5) -> list[str]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT title
        FROM tasks
        WHERE shift_id = ? AND assigned_to = ? AND status != 'completed'
        ORDER BY id
        LIMIT ?
        """,
        (shift_id, worker_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return [str(r[0] or "Задача") for r in rows]


def assignment_join_worker_name(row: tuple) -> str:
    """Имя в строке SELECT a.*, w.full_name, w.phone (full_name всегда предпоследний столбец)."""
    if len(row) >= 2 and row[-2]:
        return str(row[-2])
    return str(row[2])


def do_checkin(
    shift_id: int,
    worker_id: int,
    photo_url: str | None = None,
    checkin_lat: float | None = None,
    checkin_lng: float | None = None,
    checkin_geo_ok: int | None = None,
) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    ts = now_local_naive()
    if checkin_geo_ok is not None:
        cur.execute(
            """
            UPDATE assignments SET status = 'checked_in', checkin_time = ?, checkin_photo = ?,
                checkin_lat = ?, checkin_lng = ?, checkin_geo_ok = ?
            WHERE shift_id = ? AND worker_id = ?
        """,
            (ts, photo_url, checkin_lat, checkin_lng, checkin_geo_ok, shift_id, worker_id),
        )
    else:
        cur.execute(
            """
            UPDATE assignments SET status = 'checked_in', checkin_time = ?, checkin_photo = ?,
                checkin_lat = ?, checkin_lng = ?
            WHERE shift_id = ? AND worker_id = ?
        """,
            (ts, photo_url, checkin_lat, checkin_lng, shift_id, worker_id),
        )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def set_assignment_checkin_geo_failed(shift_id: int, worker_id: int) -> None:
    """Фиксируем неудачную попытку чек-ина по гео (вне радиуса)."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE assignments SET checkin_geo_ok = 0
        WHERE shift_id = ? AND worker_id = ? AND status = 'confirmed'
        """,
        (shift_id, worker_id),
    )
    conn.commit()
    conn.close()


def _round_hours(hours: float, mode: str) -> float:
    if hours <= 0:
        return 0.0
    mode = (mode or "none").strip().lower()
    if mode == "quarter_up":
        return (int(hours * 4 + 0.999999) / 4.0)
    if mode == "half_up":
        return (int(hours * 2 + 0.999999) / 2.0)
    if mode == "hour_up":
        return float(int(hours + 0.999999))
    return hours


def _late_checkin_penalty_hours(checkin_time: datetime, shift_date: str, start_time: str, end_time: str) -> float:
    try:
        shift_start, _ = shift_start_end_local_naive(str(shift_date), str(start_time), str(end_time))
    except Exception:
        return 0.0
    delay_sec = (checkin_time - shift_start).total_seconds()
    if 0 < delay_sec <= 30 * 60:
        return max(0.0, float(LATE_CHECKIN_PENALTY_FIRST_30M_HOURS))
    return 0.0


def do_checkout(shift_id: int, worker_id: int, photo_url: str | None = None) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT checkin_time FROM assignments
        WHERE shift_id = ? AND worker_id = ? AND status = 'checked_in'
        """,
        (shift_id, worker_id),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        conn.close()
        return False
    checkin_time = _parse_sqlite_ts(row[0])
    now = now_local_naive()
    hours_actual = max(0.0, (now - checkin_time).total_seconds() / 3600.0)

    cur.execute("SELECT shift_date, start_time, end_time, rate FROM shifts WHERE id = ?", (shift_id,))
    shift_row = cur.fetchone()
    if shift_row:
        shift_date, start_time, end_time, rate = shift_row
    else:
        shift_date, start_time, end_time, rate = "", "00:00", "00:00", 500

    penalty_h = _late_checkin_penalty_hours(checkin_time, str(shift_date), str(start_time), str(end_time))
    billed_before_round = max(0.0, hours_actual - penalty_h)
    billed_h = _round_hours(billed_before_round, BILLING_ROUNDING_MODE)
    payment = billed_h * float(rate)
    ts_out = now_local_naive()

    cur.execute(
        """
        UPDATE assignments SET status = 'checked_out', checkout_time = ?,
        checkout_photo = ?, hours_worked = ?, billed_hours = ?, penalty_hours = ?, billing_rounding_mode = ?, payment = ?
        WHERE shift_id = ? AND worker_id = ? AND status = 'checked_in'
    """,
        (
            ts_out,
            photo_url,
            hours_actual,
            billed_h,
            penalty_h,
            BILLING_ROUNDING_MODE,
            payment,
            shift_id,
            worker_id,
        ),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


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
            a.assigned_notify_sent_at, a.reminder_12h_sent_at, a.reminder_12h_repeat_last_at, a.reminder_3h_sent_at,
            a.escalation_11h_sent_at, a.escalation_1h_sent_at, a.checkin_30m_sent_at, a.checkin_15m_sent_at, a.checkout_30m_sent_at,
            a.forgot_checkout_sent_at, a.late_checkin_notified_at, a.no_confirm_flagged_at, a.no_checkin_start_notified_at,
            a.extension_request_minutes, a.extension_request_status,
            a.confirmed_shift_12h_reminder_sent_at, a.confirmed_shift_3h_reminder_sent_at,
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


def list_risky_assignments(kind: str = "all", limit: int = 100) -> list:
    """
    Риск-срез для админ-дашборда.
    kind:
      - all: no_confirm + late/no_checkin
      - no_confirm: нет подтверждения после 11ч-эскалации
      - late: зафиксировано опоздание/нет чек-ина к старту
    """
    conn = db_connect()
    cur = conn.cursor()
    base = """
        SELECT
            a.id,
            a.shift_id,
            a.worker_id,
            a.status,
            a.no_confirm_flagged_at,
            a.no_checkin_start_notified_at,
            a.late_checkin_notified_at,
            s.shift_date,
            s.start_time,
            s.end_time,
            s.location,
            p.client_id,
            COALESCE(w.full_name, CAST(a.worker_id AS TEXT)),
            p.name
        FROM assignments a
        JOIN shifts s ON s.id = a.shift_id
        JOIN projects p ON p.id = s.project_id
        LEFT JOIN workers w ON w.user_id = a.worker_id
        WHERE s.status IN ('open', 'in_progress')
    """
    if kind == "no_confirm":
        where = " AND a.no_confirm_flagged_at IS NOT NULL"
    elif kind == "late":
        where = " AND (a.no_checkin_start_notified_at IS NOT NULL OR a.late_checkin_notified_at IS NOT NULL)"
    else:
        where = (
            " AND (a.no_confirm_flagged_at IS NOT NULL OR "
            "a.no_checkin_start_notified_at IS NOT NULL OR a.late_checkin_notified_at IS NOT NULL)"
        )
    sql = (
        base
        + where
        + """
        ORDER BY s.shift_date DESC, s.start_time DESC, a.id DESC
        LIMIT ?
        """
    )
    cur.execute(sql, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def record_notification_failure(
    chat_id: int,
    context: str,
    message: str,
    error: str,
    attempts: int,
) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO notification_failures (chat_id, context, message, error, attempts)
        VALUES (?, ?, ?, ?, ?)
        """,
        (chat_id, context, message, error, attempts),
    )
    conn.commit()
    conn.close()


def acquire_scheduler_lock(lock_name: str, owner_id: str, ttl_sec: int = 90) -> bool:
    now = now_local_naive()
    expires = now + timedelta(seconds=max(30, int(ttl_sec)))
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT owner_id, expires_at FROM scheduler_locks WHERE lock_name = ?",
        (lock_name,),
    )
    row = cur.fetchone()
    if not row:
        cur.execute(
            """
            INSERT INTO scheduler_locks (lock_name, owner_id, expires_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (lock_name, owner_id, expires.isoformat(sep=" ")),
        )
        conn.commit()
        conn.close()
        return True

    cur_owner = str(row[0] or "")
    cur_exp = row[1]
    exp_dt = None
    try:
        exp_dt = _parse_sqlite_ts(cur_exp) if cur_exp else None
    except Exception:
        exp_dt = None

    can_take = (exp_dt is None) or (exp_dt <= now) or (cur_owner == owner_id)
    if can_take:
        cur.execute(
            """
            UPDATE scheduler_locks
            SET owner_id = ?, expires_at = ?, updated_at = CURRENT_TIMESTAMP
            WHERE lock_name = ?
            """,
            (owner_id, expires.isoformat(sep=" "), lock_name),
        )
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False


def log_shift_replacement(
    shift_id: int,
    old_worker_id: int,
    new_worker_id: int,
    actor_user_id: int,
    reason: str = "",
) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO shift_replacements (shift_id, old_worker_id, new_worker_id, actor_user_id, reason)
        VALUES (?, ?, ?, ?, ?)
        """,
        (shift_id, old_worker_id, new_worker_id, actor_user_id, reason[:500]),
    )
    conn.commit()
    conn.close()


def mark_assignment_event(assignment_id: int, field: str) -> None:
    allowed = {
        "assigned_notify_sent_at",
        "reminder_12h_sent_at",
        "reminder_12h_repeat_last_at",
        "reminder_3h_sent_at",
        "escalation_11h_sent_at",
        "escalation_1h_sent_at",
        "checkin_30m_sent_at",
        "checkin_15m_sent_at",
        "checkout_30m_sent_at",
        "forgot_checkout_sent_at",
        "late_checkin_notified_at",
        "no_confirm_flagged_at",
        "no_checkin_start_notified_at",
        "confirmed_shift_12h_reminder_sent_at",
        "confirmed_shift_3h_reminder_sent_at",
    }
    if field not in allowed:
        return
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE assignments SET {field} = ? WHERE id = ?",
        (now_local_naive(), assignment_id),
    )
    conn.commit()
    conn.close()


def mark_assignment_event_by_shift_worker(shift_id: int, worker_id: int, field: str) -> None:
    allowed = {
        "assigned_notify_sent_at",
        "reminder_12h_sent_at",
        "reminder_12h_repeat_last_at",
        "reminder_3h_sent_at",
        "escalation_11h_sent_at",
        "escalation_1h_sent_at",
        "checkin_30m_sent_at",
        "checkin_15m_sent_at",
        "checkout_30m_sent_at",
        "forgot_checkout_sent_at",
        "late_checkin_notified_at",
        "no_confirm_flagged_at",
        "no_checkin_start_notified_at",
        "confirmed_shift_12h_reminder_sent_at",
        "confirmed_shift_3h_reminder_sent_at",
    }
    if field not in allowed:
        return
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE assignments SET {field} = ? WHERE shift_id = ? AND worker_id = ?",
        (now_local_naive(), shift_id, worker_id),
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
        raw = str(row[0] or "").strip()
        t = datetime.strptime(raw[:5], "%H:%M")
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
    if USE_POSTGRES:
        cur.execute(
            """
        INSERT INTO tasks (shift_id, title, description, assigned_to)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """,
            (shift_id, title, description, assigned_to),
        )
        task_id = int(cur.fetchone()[0])
    else:
        cur.execute(
        """
        INSERT INTO tasks (shift_id, title, description, assigned_to)
        VALUES (?, ?, ?, ?)
    """,
        (shift_id, title, description, assigned_to),
    )
        task_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return task_id


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
    Возвращает: task_id, title, status, shift_id, shift_date, start_time, end_time, worker_name, client_rating
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
            COALESCE(w.full_name, 'Не назначен'),
            t.client_rating
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


def client_owns_task(client_user_id: int, task_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM tasks t
        JOIN shifts s ON s.id = t.shift_id
        JOIN projects p ON p.id = s.project_id
        WHERE t.id = ? AND p.client_id = ?
        """,
        (task_id, client_user_id),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def set_task_client_rating(client_user_id: int, task_id: int, rating: int) -> bool:
    if rating < 1 or rating > 5:
        return False
    if not client_owns_task(client_user_id, task_id):
        return False
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tasks
        SET client_rating = ?, client_rated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'completed' AND client_rating IS NULL
        """,
        (rating, task_id),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


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




def save_project_chat_message(project_id: int, user_id: int, user_name: str, message: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO project_chat_messages (project_id, user_id, user_name, message)
        VALUES (?, ?, ?, ?)
    """,
        (project_id, user_id, user_name, message),
    )
    conn.commit()
    conn.close()


def get_project_chat_messages(project_id: int, limit: int = 25) -> list:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_name, message, created_at FROM project_chat_messages
        WHERE project_id = ? ORDER BY created_at DESC LIMIT ?
    """,
        (project_id, limit),
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

    # Явный список колонок — порядок совпадает с индексами в services/shift_hub (не полагаемся на a.* в PG).
    cur.execute(
        """
        SELECT
            a.id, a.shift_id, a.worker_id, a.status, a.confirmed_at,
            a.checkin_time, a.checkin_photo, a.checkout_time, a.checkout_photo,
            a.hours_worked, a.payment,
            a.assigned_notify_sent_at, a.reminder_12h_sent_at, a.reminder_12h_repeat_last_at,
            a.reminder_3h_sent_at, a.escalation_11h_sent_at, a.escalation_1h_sent_at,
            a.checkin_30m_sent_at, a.checkin_15m_sent_at, a.checkout_30m_sent_at,
            a.forgot_checkout_sent_at, a.late_checkin_notified_at, a.no_confirm_flagged_at,
            a.no_checkin_start_notified_at, a.extension_request_minutes, a.extension_request_status,
            a.extension_requested_at, a.extension_resolved_at, a.checkin_lat, a.checkin_lng,
            a.checkin_geo_ok, a.billed_hours, a.penalty_hours, a.billing_rounding_mode,
            a.confirmed_shift_12h_reminder_sent_at, a.confirmed_shift_3h_reminder_sent_at,
            w.full_name, w.phone
        FROM assignments a
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

    if USE_POSTGRES:
        cur.execute("INSERT INTO projects (name, client_id) VALUES (%s, %s) RETURNING id", ("Выставка PRO Demo", base_c + 1))
        p1 = int(cur.fetchone()[0])
        cur.execute("INSERT INTO projects (name, client_id) VALUES (%s, %s) RETURNING id", ("Промо-акция Весна", base_c + 2))
        p2 = int(cur.fetchone()[0])
    else:
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
        if USE_POSTGRES:
            cur.execute(
                """
                INSERT INTO shifts (project_id, shift_date, start_time, end_time, location, rate, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                s,
            )
            shift_ids.append(int(cur.fetchone()[0]))
        else:
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
            INSERT INTO assignments (shift_id, worker_id, status)
            SELECT ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM assignments WHERE shift_id = ? AND worker_id = ?
            )
            """,
            (a[0], a[1], a[2], a[0], a[1]),
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
