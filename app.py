"""1C OData AI Agent — Streamlit chat interface."""
import os
import queue
import threading
import time
import uuid

import streamlit as st

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Secret123!")

CONFIG_OPTIONS = {
    "ut": "📦 Управление торговлей 11",
    "bp": "📊 Бухгалтерия предприятия 3.0",
}

EXAMPLE_QUESTIONS = [
    "Что происходит с продажами? Покажи динамику по месяцам за 2024 год",
    "Какие товары чаще всего возвращают?",
    "Покажи топ-5 клиентов по выручке",
    "Построй динамику по продажам для худшего магазина",
    "Где у меня есть места неэффективности?",
    "Какова маржинальность по категориям товаров?",
    "Что происходит с закупками?",
    "Покажи сезонность продаж",
]

st.set_page_config(
    page_title="1C OData Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state init ─────────────────────────────────────────────────────────
def init_state():
    defaults = {
        "authenticated": False,
        "username": "",
        "config": "ut",
        "chats": {},          # {chat_id: {"title": str, "messages": [{"role", "content", "tool_calls"}]}}
        "active_chat": None,
        "operations": {},     # {chat_id: operation_state}
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_state()


# ── Auth ───────────────────────────────────────────────────────────────────────
def login_page():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("🤖 1C OData Agent")
        st.caption("AI-аналитик для данных 1С:Предприятие")
        st.divider()
        with st.form("login"):
            user = st.text_input("Пользователь", value="admin")
            pwd = st.text_input("Пароль", type="password")
            submitted = st.form_submit_button("Войти", width="stretch", type="primary")
            if submitted:
                if user == ADMIN_USER and pwd == ADMIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.session_state.username = user
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")


def _get_draft_key(chat_id: str) -> str:
    return f"chat_draft_{chat_id}"


def _is_any_operation_running() -> bool:
    for op in st.session_state.operations.values():
        if op.get("running"):
            return True
    return False


def _is_operation_running_for_chat(chat_id: str) -> bool:
    op = st.session_state.operations.get(chat_id)
    return bool(op and op.get("running"))


def _start_operation(chat_id: str, question: str):
    from agent import stream_agent_events

    chat = st.session_state.chats[chat_id]
    config_name = st.session_state.config
    chat["messages"].append({"role": "user", "content": question})
    if len(chat["messages"]) == 1:
        title = question[:50] + ("…" if len(question) > 50 else "")
        chat["title"] = title

    event_queue: queue.Queue = queue.Queue()
    cancel_event = threading.Event()

    operation = {
        "running": True,
        "queue": event_queue,
        "cancel_event": cancel_event,
        "status_events": [],
        "thinking_chunks": [],
        "tool_calls": [],
        "final_chunks": [],
    }
    st.session_state.operations[chat_id] = operation

    def _worker():
        try:
            for event in stream_agent_events(
                question,
                config_name,
                timeout=120,
                cancel_event=cancel_event,
            ):
                event_queue.put(event)
        finally:
            event_queue.put({"type": "done"})

    threading.Thread(target=_worker, daemon=True).start()


def _drain_operation_events(chat_id: str):
    op = st.session_state.operations.get(chat_id)
    if not op or not op.get("running"):
        return

    while True:
        try:
            event = op["queue"].get_nowait()
        except queue.Empty:
            break

        if event["type"] == "thinking":
            op["thinking_chunks"].append(event["text"])
        elif event["type"] == "status":
            op["status_events"].append(event["text"])
        elif event["type"] == "tool_call":
            op["tool_calls"].append({"tool": event["tool"], "args": event["args"]})
        elif event["type"] == "final":
            op["final_chunks"].append(event["text"])
        elif event["type"] == "done":
            op["running"] = False


def _finalize_operation(chat_id: str):
    op = st.session_state.operations.get(chat_id)
    if not op or op.get("running"):
        return

    full_text = "".join(op["final_chunks"]).strip()
    if not full_text:
        thinking_text = "".join(op["thinking_chunks"]).strip()
        if thinking_text:
            full_text = f"{thinking_text}\n\n⏱ Агент завершил без финального блока. Показан промежуточный результат."
        elif op["tool_calls"]:
            full_text = "⏱ Агент не вернул итоговый ответ. Запросы к данным выполнены, смотрите детали в блоке инструментов."
        else:
            full_text = "Нет ответа."

    st.session_state.chats[chat_id]["messages"].append(
        {
            "role": "assistant",
            "content": full_text,
            "thinking": "".join(op["thinking_chunks"]),
            "thinking_count": len(op["thinking_chunks"]),
            "tool_calls": op["tool_calls"],
        }
    )
    del st.session_state.operations[chat_id]


def _cancel_operation(chat_id: str):
    op = st.session_state.operations.get(chat_id)
    if not op or not op.get("running"):
        return
    op["cancel_event"].set()
    op["running"] = False
    partial = "".join(op["final_chunks"]).strip() or "".join(op["thinking_chunks"]).strip()
    cancel_text = "⛔ Операция отменена пользователем."
    full_text = f"{partial}\n\n{cancel_text}".strip() if partial else cancel_text
    st.session_state.chats[chat_id]["messages"].append(
        {
            "role": "assistant",
            "content": full_text,
            "thinking": "".join(op["thinking_chunks"]),
            "thinking_count": len(op["thinking_chunks"]),
            "tool_calls": op["tool_calls"],
        }
    )
    del st.session_state.operations[chat_id]


# ── Sidebar ────────────────────────────────────────────────────────────────────
def sidebar():
    operation_running = _is_any_operation_running()
    with st.sidebar:
        st.title("🤖 OData Agent")
        st.caption(f"👤 {st.session_state.username}")

        st.divider()

        # Config selector
        st.subheader("Конфигурация 1С")
        selected = st.radio(
            label="Конфигурация",
            options=list(CONFIG_OPTIONS.keys()),
            format_func=lambda k: CONFIG_OPTIONS[k],
            index=list(CONFIG_OPTIONS.keys()).index(st.session_state.config),
            label_visibility="collapsed",
        )
        if selected != st.session_state.config:
            st.session_state.config = selected

        st.divider()

        # New chat
        if st.button("➕ Новый чат", width="stretch", disabled=operation_running):
            chat_id = str(uuid.uuid4())[:8]
            st.session_state.chats[chat_id] = {"title": "Новый чат", "messages": []}
            st.session_state.active_chat = chat_id
            st.rerun()

        # Chat history
        if st.session_state.chats:
            st.subheader("История чатов")
            for chat_id, chat in reversed(list(st.session_state.chats.items())):
                label = chat["title"]
                is_active = chat_id == st.session_state.active_chat
                btn_type = "primary" if is_active else "secondary"
                if st.button(
                    label,
                    key=f"chat_{chat_id}",
                    width="stretch",
                    type=btn_type,
                    disabled=operation_running and not is_active,
                ):
                    st.session_state.active_chat = chat_id
                    st.rerun()

        st.divider()
        if st.button("🚪 Выйти", width="stretch"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


# ── Chat page ──────────────────────────────────────────────────────────────────
def chat_page():
    # Ensure there's an active chat
    if not st.session_state.active_chat or st.session_state.active_chat not in st.session_state.chats:
        if st.session_state.chats:
            st.session_state.active_chat = list(st.session_state.chats.keys())[-1]
        else:
            chat_id = str(uuid.uuid4())[:8]
            st.session_state.chats[chat_id] = {"title": "Новый чат", "messages": []}
            st.session_state.active_chat = chat_id

    chat_id = st.session_state.active_chat
    chat = st.session_state.chats[chat_id]
    config = st.session_state.config
    draft_key = _get_draft_key(chat_id)
    clear_draft_key = f"clear_{draft_key}"
    prefill_draft_key = f"prefill_{draft_key}"
    if draft_key not in st.session_state:
        st.session_state[draft_key] = ""
    if clear_draft_key not in st.session_state:
        st.session_state[clear_draft_key] = False
    if prefill_draft_key not in st.session_state:
        st.session_state[prefill_draft_key] = None
    if st.session_state[clear_draft_key]:
        st.session_state[draft_key] = ""
        st.session_state[clear_draft_key] = False
    if st.session_state[prefill_draft_key] is not None:
        st.session_state[draft_key] = st.session_state[prefill_draft_key]
        st.session_state[prefill_draft_key] = None

    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"💬 {chat['title']}")
    with col2:
        st.caption(f"Конфигурация: **{CONFIG_OPTIONS[config]}**")

    _drain_operation_events(chat_id)

    # Render messages
    for msg in chat["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("thinking"):
                thinking_count = msg.get("thinking_count", 1)
                with st.expander(f"🧠 Ход рассуждений: {thinking_count}", expanded=False):
                    st.markdown(msg["thinking"])
            # Show tool calls in expander
            if msg.get("tool_calls"):
                with st.expander(f"🔧 Запросов к API: {len(msg['tool_calls'])}", expanded=False):
                    for tc in msg["tool_calls"]:
                        st.code(f"{tc['tool']}({tc['args']})", language="python")

    running = _is_operation_running_for_chat(chat_id)
    op = st.session_state.operations.get(chat_id)

    if running and op:
        with st.chat_message("assistant"):
            status = op["status_events"][-1] if op["status_events"] else "⏳ Агент анализирует данные..."
            st.markdown(status)
            if op["status_events"]:
                with st.expander(f"📡 Статус: {len(op['status_events'])}", expanded=False):
                    for s in op["status_events"]:
                        st.markdown(f"- {s}")
            if op["thinking_chunks"]:
                with st.expander(f"🧠 Ход рассуждений: {len(op['thinking_chunks'])}", expanded=False):
                    st.markdown("".join(op["thinking_chunks"]))
            if op["tool_calls"]:
                with st.expander(f"🔧 Запросов к API: {len(op['tool_calls'])}", expanded=False):
                    for tc in op["tool_calls"]:
                        st.code(f"{tc['tool']}({tc['args']})", language="python")
            if st.button("⛔ Отменить", key=f"cancel_{chat_id}", type="secondary"):
                _cancel_operation(chat_id)
                st.rerun()

    if not running and op:
        _finalize_operation(chat_id)
        st.rerun()

    with st.form(key=f"composer_{chat_id}", clear_on_submit=False):
        st.text_area("Сообщение", key=draft_key, height=100, disabled=running)
        submitted = st.form_submit_button("▶ Отправить", type="primary", disabled=running, width="stretch")
    if submitted:
        prompt = st.session_state[draft_key].strip()
        if prompt:
            st.session_state[clear_draft_key] = True
            _start_operation(chat_id, prompt)
            st.rerun()
        else:
            st.warning("Введите вопрос перед отправкой.")

    st.markdown("### С чего начать?")
    cols = st.columns(2)
    for i, q in enumerate(EXAMPLE_QUESTIONS):
        with cols[i % 2]:
            if st.button(q, key=f"example_{chat_id}_{i}", width="stretch", disabled=running):
                st.session_state[prefill_draft_key] = q
                st.rerun()
    if running:
        time.sleep(0.5)
        st.rerun()


# ── Analytics page ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _load_sales(config: str) -> "pd.DataFrame":
    import pandas as pd
    from odata.client import fetch_entity
    try:
        payload = fetch_entity(config, "AccumulationRegister_Продажи", select="Period,Сумма", top=1000)
        records = payload.get("value", [])
    except Exception:
        return pd.DataFrame(columns=["Period", "Сумма"])
    if not records:
        return pd.DataFrame(columns=["Period", "Сумма"])
    df = pd.DataFrame(records)
    df["Period"] = pd.to_datetime(df["Period"], errors="coerce")
    df["Сумма"] = pd.to_numeric(df["Сумма"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=300)
def _load_sales_by_product(config: str) -> "pd.DataFrame":
    import pandas as pd
    from odata.client import fetch_entity
    try:
        payload = fetch_entity(config, "AccumulationRegister_Продажи", select="Номенклатура_Key,Сумма", top=1000)
        records = payload.get("value", [])
    except Exception:
        return pd.DataFrame(columns=["Номенклатура_Key", "Сумма"])
    if not records:
        return pd.DataFrame(columns=["Номенклатура_Key", "Сумма"])
    df = pd.DataFrame(records)
    df["Сумма"] = pd.to_numeric(df["Сумма"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=300)
def _load_sales_by_warehouse(config: str) -> "pd.DataFrame":
    import pandas as pd
    from odata.client import fetch_entity
    try:
        payload = fetch_entity(config, "AccumulationRegister_Продажи", select="Склад_Key,Сумма", top=1000)
        records = payload.get("value", [])
    except Exception:
        return pd.DataFrame(columns=["Склад_Key", "Сумма"])
    if not records:
        return pd.DataFrame(columns=["Склад_Key", "Сумма"])
    df = pd.DataFrame(records)
    df["Сумма"] = pd.to_numeric(df["Сумма"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=300)
def _load_returns(config: str) -> "pd.DataFrame":
    import pandas as pd
    from odata.client import fetch_entity
    try:
        payload = fetch_entity(config, "Document_ВозвратТоваровОтКлиента", select="Контрагент_Key,Сумма", top=500)
        records = payload.get("value", [])
    except Exception:
        return pd.DataFrame(columns=["Контрагент_Key", "Сумма"])
    if not records:
        return pd.DataFrame(columns=["Контрагент_Key", "Сумма"])
    df = pd.DataFrame(records)
    df["Сумма"] = pd.to_numeric(df["Сумма"], errors="coerce").fillna(0)
    return df.groupby("Контрагент_Key")["Сумма"].sum().reset_index()


def analytics_page():
    import pandas as pd
    config = st.session_state.config

    st.title(f"📈 Аналитика — {CONFIG_OPTIONS[config]}")
    st.caption("Данные кешируются на 5 минут. Переключите конфигурацию в боковой панели.")

    # ── Продажи по месяцам ────────────────────────────────────────────────────
    st.subheader("Продажи по месяцам")
    with st.spinner("Загрузка..."):
        df_sales = _load_sales(config)
    if df_sales.empty:
        st.info("Нет данных о продажах.")
    else:
        monthly = (
            df_sales.dropna(subset=["Period"])
            .assign(month=lambda d: d["Period"].dt.to_period("M").astype(str))
            .groupby("month")["Сумма"].sum()
            .reset_index()
            .set_index("month")
        )
        st.line_chart(monthly)
        with st.expander("Данные"):
            st.dataframe(monthly.reset_index(), width="stretch")

    # ── Топ-10 товаров по выручке ─────────────────────────────────────────────
    st.subheader("Топ-10 товаров по выручке")
    with st.spinner("Загрузка..."):
        df_prod = _load_sales_by_product(config)
    if df_prod.empty:
        st.info("Нет данных.")
    else:
        top10 = (
            df_prod.groupby("Номенклатура_Key")["Сумма"].sum()
            .nlargest(10)
            .reset_index()
            .set_index("Номенклатура_Key")
        )
        st.bar_chart(top10)
        with st.expander("Данные"):
            st.dataframe(top10.reset_index(), width="stretch")

    # ── Продажи по складам ────────────────────────────────────────────────────
    st.subheader("Продажи по складам")
    with st.spinner("Загрузка..."):
        df_wh = _load_sales_by_warehouse(config)
    if df_wh.empty:
        st.info("Нет данных.")
    else:
        by_wh = (
            df_wh.groupby("Склад_Key")["Сумма"].sum()
            .reset_index()
            .set_index("Склад_Key")
        )
        st.bar_chart(by_wh)
        with st.expander("Данные"):
            st.dataframe(by_wh.reset_index(), width="stretch")

    # ── Возвраты по товарам (UT only) ─────────────────────────────────────────
    if config == "ut":
        st.subheader("Возвраты по контрагентам")
        with st.spinner("Загрузка..."):
            df_ret = _load_returns(config)
        if df_ret.empty:
            st.info("Нет данных о возвратах.")
        else:
            st.bar_chart(df_ret.set_index("Контрагент_Key"))
            with st.expander("Данные"):
                st.dataframe(df_ret, width="stretch")


# ── Main ───────────────────────────────────────────────────────────────────────
if not st.session_state.authenticated:
    login_page()
else:
    sidebar()
    tab_chat, tab_analytics = st.tabs(["💬 Чат", "📈 Аналитика"])
    with tab_chat:
        chat_page()
    with tab_analytics:
        analytics_page()
