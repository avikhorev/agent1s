import importlib
import json
import os

_PSYCOPG2 = None


def _chat_db_dsn() -> str:
    return os.getenv("CHAT_DB_DSN", "").strip()


def _connect():
    dsn = _chat_db_dsn()
    if not dsn:
        raise RuntimeError("CHAT_DB_DSN is not configured")

    module = _PSYCOPG2
    if module is None:
        module = importlib.import_module("psycopg2")
    return module.connect(dsn)


def init_store() -> None:
    if not _chat_db_dsn():
        return
    sql = """
    CREATE TABLE IF NOT EXISTS chat_sessions (
        username TEXT NOT NULL,
        config_name TEXT NOT NULL DEFAULT 'ut',
        chat_id TEXT NOT NULL,
        title TEXT NOT NULL,
        messages_json TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (username, config_name, chat_id)
    )
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            # Migrate from old schema where config_name did not exist.
            cur.execute("ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS config_name TEXT")
            cur.execute("UPDATE chat_sessions SET config_name = 'ut' WHERE config_name IS NULL")
            cur.execute("ALTER TABLE chat_sessions ALTER COLUMN config_name SET NOT NULL")
            cur.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE table_name = 'chat_sessions'
                          AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        ALTER TABLE chat_sessions DROP CONSTRAINT chat_sessions_pkey;
                    END IF;
                    ALTER TABLE chat_sessions
                    ADD CONSTRAINT chat_sessions_pkey
                    PRIMARY KEY (username, config_name, chat_id);
                EXCEPTION
                    WHEN duplicate_table THEN
                        NULL;
                END $$;
                """
            )
        conn.commit()


def save_chat(username: str, config_name: str, chat_id: str, title: str, messages: list[dict]) -> None:
    if not _chat_db_dsn():
        return
    payload = json.dumps(messages, ensure_ascii=False)
    sql = """
    INSERT INTO chat_sessions (username, config_name, chat_id, title, messages_json, updated_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (username, config_name, chat_id) DO UPDATE
    SET title = EXCLUDED.title,
        messages_json = EXCLUDED.messages_json,
        updated_at = NOW()
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (username, config_name, chat_id, title, payload))
        conn.commit()


def load_chats(username: str, config_name: str) -> dict[str, dict]:
    if not _chat_db_dsn():
        return {}

    sql = """
    SELECT chat_id, title, messages_json
    FROM chat_sessions
    WHERE username = %s
      AND config_name = %s
    ORDER BY updated_at ASC
    """
    chats: dict[str, dict] = {}
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (username, config_name))
            rows = cur.fetchall()

    for chat_id, title, messages_json in rows:
        try:
            messages = json.loads(messages_json) if messages_json else []
            if not isinstance(messages, list):
                messages = []
        except Exception:
            messages = []
        chats[chat_id] = {"title": title, "messages": messages}
    return chats
