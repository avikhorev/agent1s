# 1C OData AI Agent

AI-агент для анализа бизнес-данных 1С:Предприятие через чат-интерфейс.

## Быстрый старт

### 1. Настройте переменные окружения

```bash
cp .env.example .env
# Откройте .env и задайте ключ LLM-провайдера:
# - OPENROUTER_API_KEY (рекомендуется для free-моделей), или
# - ANTHROPIC_API_KEY (прямой Anthropic API)
```

### 2. Запустите

```bash
docker compose up --build
```

Через ~2 минуты (пока сидируется база) откройте **http://localhost:8501**

Логин: `admin` / `Secret123!`

## Что умеет агент

Задавайте вопросы на естественном языке:

- *«Что происходит с продажами? Покажи динамику по месяцам»*
- *«Какие товары чаще всего возвращают?»*
- *«Построй динамику по продажам для худшего магазина»*
- *«Покажи топ-5 клиентов по выручке»*
- *«Какова маржинальность по категориям товаров?»*
- *«Где у меня есть места неэффективности?»*

## Архитектура

```
Streamlit app (port 8501)
    │
    ├── Claude Agent SDK (in-process)
    │       └── OData MCP tools
    │               └── HTTP → OData Mock Server (port 8080)
    │                               └── PostgreSQL 15
    └── session_state + PostgreSQL chat_sessions
            (история чатов по username + config_name)
```

**Стек:**
- [Streamlit](https://streamlit.io/) — UI и сессии
- [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk) — автономный агент
- [OpenRouter](https://openrouter.ai/) или Anthropic API — LLM-провайдер
- FastAPI OData mock server — эмулятор 1С:Предприятие 8.3

## Конфигурации 1С

| Конфигурация | Описание |
|---|---|
| **UT** — Управление торговлей 11 | Номенклатура, контрагенты, склады, продажи (~8000 документов), закупки, возвраты, цены |
| **BP** — Бухгалтерия предприятия 3.0 | Реализации, поступления, платежи, взаиморасчёты, подразделения |

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `OPENROUTER_API_KEY` | — | API ключ OpenRouter (если используете OpenRouter) |
| `ANTHROPIC_BASE_URL` | `https://openrouter.ai/api` | URL провайдера для SDK (оставьте пустым для прямого Anthropic) |
| `ANTHROPIC_AUTH_TOKEN` | `${OPENROUTER_API_KEY}` | Токен авторизации для SDK при OpenRouter |
| `ANTHROPIC_API_KEY` | `""` | Ключ Anthropic API (при прямом Anthropic) |
| `ANTHROPIC_MODEL` | `openrouter/free` | Модель для агента |
| `ADMIN_USER` | `admin` | Логин |
| `ADMIN_PASSWORD` | `Secret123!` | Пароль |
| `ODATA_MOCK_URL` | `http://odata-mock:8080` | URL OData сервера |
| `CHAT_DB_DSN` | `postgresql://odata:odata_secret@odata-postgres:5432/odata_1c` | PostgreSQL для хранения истории чатов |

## Локальная разработка

```bash
pip install -r requirements.txt

# Запустите OData mock отдельно:
cd vendor/coreai_1c_test_server
docker compose up

# Затем запустите Streamlit:
ODATA_MOCK_URL=http://localhost:8080 \
CHAT_DB_DSN=postgresql://odata:odata_secret@localhost:5438/odata_1c \
streamlit run app.py
```

## Структура проекта

```
app.py              # Streamlit UI + auth + chat (200 строк)
agent.py            # Claude Agent SDK runner (80 строк)
services/
└── chat_store.py   # Хранилище истории чатов в PostgreSQL (username + config)
odata/
├── client.py       # HTTP клиент для OData API
├── tools.py        # Функции инструментов агента
├── mcp_server.py   # MCP-обёртки для Claude SDK
├── metadata.py     # Парсер EDMX схемы
└── types.py        # Типы данных
docker-compose.yml  # Streamlit app + OData mock + PostgreSQL
Dockerfile
requirements.txt
vendor/             # 1C OData Mock Server
```
