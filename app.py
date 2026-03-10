"""1C OData AI Agent — Streamlit chat interface."""
import logging
import os
import queue
import re
import threading
import time
import uuid

import streamlit as st

logger = logging.getLogger(__name__)

ADMIN_USER = os.getenv("ADMIN_USER", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()

CONFIG_OPTIONS = {
    "ut": "📦 Управление торговлей 11",
    "bp": "📊 Бухгалтерия предприятия 3.0",
}

_SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

EXAMPLE_QUESTIONS_BY_CONFIG = {
    "ut": [
        "Что происходит с продажами за последние 12 месяцев?",
        "Какие товары чаще всего возвращают?",
        "Покажи топ-5 клиентов по выручке",
        "Какие товары дают максимум выручки?",
        "Где у меня есть места неэффективности?",
        "Какова маржинальность по категориям товаров?",
        "Что происходит с закупками?",
        "Покажи сезонность продаж",
    ],
    "bp": [
        "Покажи топ-5 контрагентов по выручке",
        "Какая динамика оплат от покупателей по месяцам?",
        "Какая дебиторская задолженность на сегодня?",
        "Какая кредиторская задолженность на сегодня?",
        "Какие подразделения дают максимум выручки?",
        "Какие контрагенты платят с наибольшей задержкой?",
        "Сравни реализации и поступления денежных средств",
        "Какие документы реализации самые крупные?",
    ],
}

st.set_page_config(
    page_title="1C OData Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* Hide Streamlit's built-in top-right running indicator (multiple selectors for version compat) */
    div[data-testid="stStatusWidget"],
    div[data-testid="stStatusWidget"] *,
    .stStatusWidget,
    [class*="StatusWidget"],
    header[data-testid="stHeader"] .stToolbar { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
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
                if not ADMIN_USER or not ADMIN_PASSWORD:
                    st.error("Сервер не настроен: задайте ADMIN_USER и ADMIN_PASSWORD.")
                    return
                if user == ADMIN_USER and pwd == ADMIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.session_state.username = user
                    _restore_user_chats(user, st.session_state.config)
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")


def _get_draft_key(chat_id: str) -> str:
    return f"chat_draft_{chat_id}"


_QUESTION_WORDS_RU = {"какие", "какой", "какая", "какое", "что", "где", "как", "когда",
                       "кто", "почему", "зачем", "сколько", "каких", "каким", "укажите",
                       "помогите", "уточните", "расскажите"}

# Meta-options that are UI actions, not user messages — always filtered out
_META_OPTION_FRAGMENTS = {"указать свой", "свой период", "другой вариант", "пользовательский",
                           "уточнить", "другое", "иное", "произвольн"}


def _extract_suggestions(content: str) -> list[str]:
    """Extract clickable options from assistant message.

    Priority 1: structured [OPTIONS: a | b | c] tag emitted by the LLM.
    Priority 2: heuristic extraction (only if no OPTIONS tag found).
    """
    # Priority 1: explicit LLM tag
    m = re.search(r"\[OPTIONS:\s*(.+?)\]", content, re.IGNORECASE)
    if m:
        raw = m.group(1)
        options = [o.strip().rstrip("?").strip() for o in raw.split("|") if o.strip()]
        options = [o for o in options if 2 < len(o) <= 60]
        options = [o for o in options if not any(f in o.lower() for f in _META_OPTION_FRAGMENTS)]
        return options[:4]

    # Priority 2: heuristic extraction
    seen: set[str] = set()
    suggestions: list[str] = []

    def _add(text: str):
        text = re.sub(r"\*+", "", text)
        text = re.sub(r"^[•\-\*\d]+[.)]\s*", "", text.strip()).strip()
        text = text.rstrip("?:,;. ").strip()
        text = re.sub(r"\s+и\s+т\.?\s*д\.?.*$", "", text).strip()
        if not text or len(text) < 3 or len(text) > 80:
            return
        first = text.split()[0].lower().rstrip("?:,") if text.split() else ""
        if first in _QUESTION_WORDS_RU:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        suggestions.append(text)

    for quoted in re.findall(r"«([^»]+)»", content):
        _add(quoted)

    for paren in re.findall(r"\(([^)]{5,150})\)", content):
        parts = [p.strip() for p in paren.split(",")]
        if len(parts) >= 2:
            for part in parts:
                _add(part)

    for line in content.splitlines():
        lm = re.match(r"^(?:[•\-\*]|\d+[.)]) +(.+)", line.strip())
        if lm:
            item = lm.group(1).strip()
            first = item.split()[0].lower().rstrip("?:,") if item.split() else ""
            if first not in _QUESTION_WORDS_RU and len(item) < 80:
                _add(item)

    return suggestions


def _strip_options_tag(content: str) -> str:
    """Remove [OPTIONS: ...] tag from displayed message content."""
    return re.sub(r"\n?\[OPTIONS:[^\]]*\]", "", content, flags=re.IGNORECASE).rstrip()


def _render_suggestions(content: str, chat_id: str, msg_idx: int, running: bool):
    """Render clickable suggestion buttons extracted from assistant message."""
    suggestions = _extract_suggestions(content)
    if not suggestions:
        return
    prefill_key = f"prefill_{_get_draft_key(chat_id)}"
    cols = st.columns(min(len(suggestions), 3))
    for i, suggestion in enumerate(suggestions):
        with cols[i % len(cols)]:
            if st.button(
                suggestion,
                key=f"suggest_{chat_id}_{msg_idx}_{i}",
                disabled=running,
                width="stretch",
            ):
                st.session_state[prefill_key] = suggestion
                st.rerun()


def _is_any_operation_running() -> bool:
    for op in st.session_state.operations.values():
        if op.get("running"):
            return True
    return False


def _is_operation_running_for_chat(chat_id: str) -> bool:
    op = st.session_state.operations.get(chat_id)
    return bool(op and op.get("running"))


def _restore_user_chats(username: str, config_name: str):
    try:
        from services.chat_store import init_store, load_chats

        init_store()
        chats = load_chats(username, config_name)
        if chats:
            st.session_state.chats = chats
            st.session_state.active_chat = list(chats.keys())[-1]
        else:
            st.session_state.chats = {}
            st.session_state.active_chat = None
    except Exception:
        # DB storage is optional outside docker/local DB runs.
        logger.exception("Failed to restore chats from DB store")
        st.session_state.chats = {}
        st.session_state.active_chat = None


def _persist_chat(chat_id: str):
    if chat_id not in st.session_state.chats:
        return
    try:
        from services.chat_store import init_store, save_chat

        init_store()
        chat = st.session_state.chats[chat_id]
        save_chat(
            st.session_state.username,
            st.session_state.config,
            chat_id,
            chat["title"],
            chat["messages"],
        )
    except Exception:
        logger.exception("Failed to persist chat to DB store")


def _start_operation(chat_id: str, question: str):
    from agent import stream_agent_events

    chat = st.session_state.chats[chat_id]
    config_name = st.session_state.config
    chat["messages"].append({"role": "user", "content": question})
    if len(chat["messages"]) == 1:
        title = question[:50] + ("…" if len(question) > 50 else "")
        chat["title"] = title
    _persist_chat(chat_id)

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
                history=chat["messages"],
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
    # ResultMessage often includes all prior thinking text as a prefix — strip it.
    thinking_prefix = "".join(op["thinking_chunks"]).strip()
    if thinking_prefix and full_text.startswith(thinking_prefix):
        full_text = full_text[len(thinking_prefix):].strip()

    # Build thinking display: each chunk is one reasoning step, joined with dividers.
    # Also strip the final answer from the end so it doesn't duplicate in the expander.
    thinking_steps = [c.strip() for c in op["thinking_chunks"] if c.strip()]
    if full_text and thinking_steps and thinking_steps[-1] == full_text:
        thinking_steps = thinking_steps[:-1]
    elif full_text and thinking_steps:
        last = thinking_steps[-1]
        if last.endswith(full_text):
            trimmed = last[: -len(full_text)].strip()
            thinking_steps = thinking_steps[:-1] + ([trimmed] if trimmed else [])
    thinking_display = "\n\n---\n\n".join(thinking_steps)

    if not full_text:
        if thinking_display:
            full_text = thinking_display
        elif op["tool_calls"]:
            full_text = "Запросы к данным выполнены, смотрите детали ниже."
        else:
            full_text = "Нет ответа."

    # Always keep the raw thinking for the collapsed expander, even if it became the answer.
    raw_thinking = "\n\n---\n\n".join(c.strip() for c in op["thinking_chunks"] if c.strip())

    st.session_state.chats[chat_id]["messages"].append(
        {
            "role": "assistant",
            "content": full_text,
            "thinking": raw_thinking,
            "thinking_count": len(op["thinking_chunks"]),
            "tool_calls": op["tool_calls"],
        }
    )
    _persist_chat(chat_id)
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
    _persist_chat(chat_id)
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
            disabled=operation_running,
        )
        if selected != st.session_state.config:
            st.session_state.config = selected
            if st.session_state.authenticated:
                _restore_user_chats(st.session_state.username, selected)

        st.divider()

        # New chat
        if st.button("➕ Новый чат", width="stretch", disabled=operation_running):
            chat_id = str(uuid.uuid4())[:8]
            st.session_state.chats[chat_id] = {"title": "Новый чат", "messages": []}
            st.session_state.active_chat = chat_id
            _persist_chat(chat_id)
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
            _persist_chat(chat_id)

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
    _drain_operation_events(chat_id)
    running = _is_operation_running_for_chat(chat_id)

    model_name = os.getenv("ANTHROPIC_MODEL", "claude")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"💬 {chat['title']}")
    with col2:
        spin_char = _SPIN[int(time.time() * 6) % len(_SPIN)] if running else ""
        st.caption(f"{'`' + spin_char + '`  ' if spin_char else ''}Модель: **{model_name}**")
    op = st.session_state.operations.get(chat_id)

    last_assistant_idx = max(
        (i for i, m in enumerate(chat["messages"]) if m["role"] == "assistant"),
        default=-1,
    )

    # Render messages
    for msg_idx, msg in enumerate(chat["messages"]):
        with st.chat_message(msg["role"]):
            st.markdown(_strip_options_tag(msg["content"]))
            if msg.get("thinking"):
                thinking_count = msg.get("thinking_count", 1)
                with st.expander(f"🧠 Ход рассуждений: {thinking_count}", expanded=False):
                    st.markdown(msg["thinking"])
            # Show tool calls in expander
            if msg.get("tool_calls"):
                with st.expander(f"🔧 Запросов к API: {len(msg['tool_calls'])}", expanded=False):
                    for tc in msg["tool_calls"]:
                        st.code(f"{tc['tool']}({tc['args']})", language="python")
            if msg["role"] == "assistant" and msg_idx == last_assistant_idx:
                _render_suggestions(msg["content"], chat_id, msg_idx, running)

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
            spin = _SPIN[int(time.time() * 6) % len(_SPIN)]
            if st.button(f"{spin} Отменить", key=f"cancel_{chat_id}", type="secondary"):
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
    questions = EXAMPLE_QUESTIONS_BY_CONFIG.get(config, EXAMPLE_QUESTIONS_BY_CONFIG["ut"])
    for i, q in enumerate(questions):
        with cols[i % 2]:
            if st.button(q, key=f"example_{chat_id}_{i}", width="stretch", disabled=running):
                st.session_state[prefill_draft_key] = q
                st.rerun()
    if running:
        time.sleep(0.5)
        st.rerun()


# ── Analytics page ─────────────────────────────────────────────────────────────
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
    config = st.session_state.config

    st.title(f"📈 Аналитика — {CONFIG_OPTIONS[config]}")
    st.caption("Данные кешируются на 5 минут. Переключите конфигурацию в боковой панели.")

    # ── Топ товаров и склады — pie charts ─────────────────────────────────────
    with st.spinner("Загрузка..."):
        df_prod = _load_sales_by_product(config)
        df_wh = _load_sales_by_warehouse(config)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Доля выручки по товарам")
        if df_prod.empty:
            st.info("Нет данных.")
        else:
            import plotly.express as px
            grouped = df_prod.groupby("Номенклатура_Key")["Сумма"].sum().reset_index()
            top8 = grouped.nlargest(8, "Сумма")
            rest = grouped["Сумма"].sum() - top8["Сумма"].sum()
            if rest > 0:
                import pandas as pd
                top8 = pd.concat([
                    top8,
                    pd.DataFrame([{"Номенклатура_Key": "Прочие", "Сумма": rest}]),
                ], ignore_index=True)
            fig = px.pie(top8, values="Сумма", names="Номенклатура_Key",
                         hole=0.35)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig, width="stretch")
            with st.expander("Данные"):
                st.dataframe(top8, width="stretch")

    with col_right:
        st.subheader("Продажи по складам")
        if df_wh.empty:
            st.info("Нет данных.")
        else:
            import plotly.express as px
            by_wh = df_wh.groupby("Склад_Key")["Сумма"].sum().reset_index()
            fig2 = px.pie(by_wh, values="Сумма", names="Склад_Key",
                          hole=0.35)
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig2, width="stretch")
            with st.expander("Данные"):
                st.dataframe(by_wh, width="stretch")

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
