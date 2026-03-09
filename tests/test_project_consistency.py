from pathlib import Path


def test_env_example_contains_runtime_variables():
    env_text = Path(".env.example").read_text(encoding="utf-8")
    assert "ODATA_MOCK_URL=" in env_text
    assert "ANTHROPIC_MODEL=claude-haiku-4-5-20251001" in env_text
    assert "ANTHROPIC_DEFAULT_HAIKU_MODEL" not in env_text


def test_compose_has_anthropic_runtime_mapping():
    compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "ANTHROPIC_MODEL" in compose_text
    assert "ANTHROPIC_DEFAULT_HAIKU_MODEL" not in compose_text


def test_ui_file_has_no_deprecated_use_container_width():
    app_text = Path("app.py").read_text(encoding="utf-8")
    assert "use_container_width=" not in app_text
    assert 'label=""' not in app_text


def test_agent_prefers_anthropic_model_env():
    agent_text = Path("agent.py").read_text(encoding="utf-8")
    assert "ANTHROPIC_MODEL" in agent_text


def test_chat_ui_has_cancel_and_free_text_input():
    app_text = Path("app.py").read_text(encoding="utf-8")
    assert "Отменить" in app_text
    assert 'st.text_area("Сообщение"' in app_text
    assert "chat_input(" not in app_text
    assert "prefill_draft_key" in app_text
    assert "st.session_state[prefill_draft_key] = q" in app_text
    assert 'st.button("➕ Новый чат", width="stretch", disabled=operation_running)' in app_text
    assert 'with st.expander(f"🧠 Ход рассуждений: {len(op[\'thinking_chunks\'])}", expanded=False):' in app_text
    assert 'with st.expander(f"🔧 Запросов к API: {len(op[\'tool_calls\'])}", expanded=False):' in app_text
