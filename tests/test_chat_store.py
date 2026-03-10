import json
import types


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if "SELECT chat_id, title, messages_json" in sql:
            self._rows = [
                (
                    "chat-1",
                    "История",
                    json.dumps([{"role": "user", "content": "q"}], ensure_ascii=False),
                )
            ]

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self):
        self.executed = []
        self.committed = False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_chat_store_saves_and_loads_json_history(monkeypatch):
    import services.chat_store as store

    fake_conn = _FakeConn()
    fake_psycopg2 = types.SimpleNamespace(connect=lambda dsn: fake_conn)
    monkeypatch.setenv("CHAT_DB_DSN", "postgres://test")
    monkeypatch.setattr(store, "_PSYCOPG2", fake_psycopg2, raising=False)

    messages = [{"role": "user", "content": "hello"}]
    store.init_store()
    store.save_chat("admin", "ut", "chat-1", "История", messages)
    chats = store.load_chats("admin", "ut")

    assert "chat-1" in chats
    assert chats["chat-1"]["title"] == "История"
    assert chats["chat-1"]["messages"][0]["content"] == "q"
    assert fake_conn.committed is True
    joined_sql = "\n".join(sql for sql, _ in fake_conn.executed)
    assert "CREATE TABLE IF NOT EXISTS chat_sessions" in joined_sql
    assert "config_name" in joined_sql
    assert "INSERT INTO chat_sessions" in joined_sql
    assert "ON CONFLICT (username, config_name, chat_id) DO UPDATE" in joined_sql
