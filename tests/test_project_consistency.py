from pathlib import Path


def test_env_example_contains_runtime_variables():
    env_text = Path(".env.example").read_text(encoding="utf-8")
    assert "OPENROUTER_API_KEY=" in env_text
    assert "ODATA_MOCK_URL=" in env_text
    assert "ANTHROPIC_DEFAULT_HAIKU_MODEL=stepfun/step-3.5-flash:free" in env_text


def test_compose_uses_openrouter_mapping():
    compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "ANTHROPIC_BASE_URL: ${ANTHROPIC_BASE_URL:-https://openrouter.ai/api}" in compose_text
    assert "ANTHROPIC_AUTH_TOKEN: ${OPENROUTER_API_KEY}" in compose_text
    assert 'ANTHROPIC_API_KEY: ""' in compose_text


def test_ui_file_has_no_deprecated_use_container_width():
    app_text = Path("app.py").read_text(encoding="utf-8")
    assert "use_container_width=" not in app_text
    assert 'label=""' not in app_text

