"""1C OData AI Agent — Streamlit chat interface."""
import os
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
        "thinking": False,
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
            submitted = st.form_submit_button("Войти", use_container_width=True, type="primary")
            if submitted:
                if user == ADMIN_USER and pwd == ADMIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.session_state.username = user
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")


# ── Sidebar ────────────────────────────────────────────────────────────────────
def sidebar():
    with st.sidebar:
        st.title("🤖 OData Agent")
        st.caption(f"👤 {st.session_state.username}")

        st.divider()

        # Config selector
        st.subheader("Конфигурация 1С")
        selected = st.radio(
            label="",
            options=list(CONFIG_OPTIONS.keys()),
            format_func=lambda k: CONFIG_OPTIONS[k],
            index=list(CONFIG_OPTIONS.keys()).index(st.session_state.config),
            label_visibility="collapsed",
        )
        if selected != st.session_state.config:
            st.session_state.config = selected

        st.divider()

        # New chat
        if st.button("➕ Новый чат", use_container_width=True):
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
                if st.button(label, key=f"chat_{chat_id}", use_container_width=True, type=btn_type):
                    st.session_state.active_chat = chat_id
                    st.rerun()

        st.divider()
        if st.button("🚪 Выйти", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


# ── Chat page ──────────────────────────────────────────────────────────────────
def chat_page():
    sidebar()

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

    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"💬 {chat['title']}")
    with col2:
        st.caption(f"Конфигурация: **{CONFIG_OPTIONS[config]}**")

    # Empty state with examples
    if not chat["messages"]:
        st.markdown("### С чего начать?")
        cols = st.columns(2)
        for i, q in enumerate(EXAMPLE_QUESTIONS):
            with cols[i % 2]:
                if st.button(q, key=f"example_{i}", use_container_width=True):
                    _send_message(chat_id, q)
                    st.rerun()
        return

    # Render messages
    for msg in chat["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # Show tool calls in expander
            if msg.get("tool_calls"):
                with st.expander(f"🔧 Запросов к API: {len(msg['tool_calls'])}", expanded=False):
                    for tc in msg["tool_calls"]:
                        st.code(f"{tc['tool']}({tc['args']})", language="python")

    # Thinking indicator
    if st.session_state.thinking:
        with st.chat_message("assistant"):
            with st.spinner("Агент анализирует данные..."):
                time.sleep(0.1)
        st.rerun()

    # Input
    if prompt := st.chat_input("Задайте вопрос о ваших данных...", disabled=st.session_state.thinking):
        _send_message(chat_id, prompt)
        st.rerun()


def _send_message(chat_id: str, question: str):
    from agent import run_agent

    chat = st.session_state.chats[chat_id]

    # Add user message
    chat["messages"].append({"role": "user", "content": question})

    # Update chat title from first question
    if len(chat["messages"]) == 1:
        title = question[:50] + ("…" if len(question) > 50 else "")
        chat["title"] = title

    # Run agent synchronously (Streamlit runs in a thread, asyncio.run() is fine)
    with st.spinner("Агент анализирует данные..."):
        result = run_agent(question, st.session_state.config)

    chat["messages"].append({
        "role": "assistant",
        "content": result["answer"],
        "tool_calls": result.get("tool_calls", []),
    })


# ── Main ───────────────────────────────────────────────────────────────────────
if not st.session_state.authenticated:
    login_page()
else:
    chat_page()
