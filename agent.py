"""Claude Agent SDK runner for 1C OData queries."""
import asyncio
import os
import queue
import re
import subprocess
import threading
import time

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)

from odata.mcp_server import create_odata_mcp_server
from odata.tools import (
    monthly_sales_summary,
    top_customers_by_revenue,
    top_products_by_revenue,
    top_returned_products,
)

_mcp_server = None


def _get_mcp_server():
    global _mcp_server
    if _mcp_server is None:
        _mcp_server = create_odata_mcp_server()
    return _mcp_server


MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
FALLBACK_MODEL = os.getenv("ANTHROPIC_FALLBACK_MODEL", "claude-sonnet-4-5")
FIRST_TOKEN_TIMEOUT_SEC = float(os.getenv("AGENT_FIRST_TOKEN_TIMEOUT_SEC", "8"))
WARM_STATUS_INTERVAL_SEC = int(os.getenv("AGENT_WARM_STATUS_INTERVAL_SEC", "300"))
ALLOWED_TOOLS = [
    "mcp__odata__list_configs",
    "mcp__odata__list_entities",
    "mcp__odata__describe_entity",
    "mcp__odata__query_entity",
    "mcp__odata__get_by_key",
    "mcp__odata__top_customers_by_revenue",
    "mcp__odata__top_products_by_revenue",
    "mcp__odata__monthly_sales_summary",
    "mcp__odata__top_returned_products",
]
DISALLOWED_TOOLS = [
    "Agent",
    "Task",
    "TaskOutput",
    "Bash",
    "WebSearch",
    "Glob",
    "Grep",
    "LS",
    "Read",
    "Edit",
    "Write",
]
_warm_thread_started = False


def _normalize_config(config_name: str | None) -> str:
    return (config_name or "").strip().lower()


def _build_tool_guard(selected_config: str):
    selected = _normalize_config(selected_config)

    async def _can_use_tool(tool_name: str, tool_input: dict, _context):
        if not tool_name.startswith("mcp__odata__"):
            return PermissionResultDeny(message=f"Tool '{tool_name}' is not allowed")

        if tool_name == "mcp__odata__list_configs":
            return PermissionResultAllow()

        updated_input = dict(tool_input or {})
        current = _normalize_config(updated_input.get("config_name", selected))
        if current != selected:
            return PermissionResultDeny(
                message=f"Only config '{selected}' is allowed in this chat (got '{current}')."
            )
        updated_input["config_name"] = selected
        return PermissionResultAllow(updated_input=updated_input)

    return _can_use_tool


def _system_prompt(config_name: str) -> str:
    cfg = _normalize_config(config_name)
    cfg_context = {
        "ut": (
            "UT (Управление торговлей): Catalog_Контрагенты, "
            "Document_РеализацияТоваровУслуг, AccumulationRegister_Продажи."
        ),
        "bp": (
            "BP (Бухгалтерия): Catalog_Контрагенты, "
            "Document_РеализацияТоваровУслуг, AccumulationRegister_ВзаиморасчетыСКонтрагентами."
        ),
    }.get(cfg, cfg)

    return f"""Ты — аналитический AI-агент 1С OData. Активная конфигурация: {cfg_context}

Правила оптимизации (обязательно):
1) До первого запроса оцени число API вызовов и выбери минимальный план.
2) Если оценка > 20 вызовов: используй специализированные инструменты/агрегацию, без мелкой пагинации.
3) Для частых задач используй: `top_customers_by_revenue`, `top_products_by_revenue`, `monthly_sales_summary`, `top_returned_products`.
4) Для тяжёлых сущностей всегда ограничивай период дат.
5) Не переключай конфигурацию и не делай лишние introspection-вызовы.

Формат ответа:
- Русский язык, кратко.
- Для ранжирования/динамики — markdown-таблицы.
"""


def _try_direct_answer(question: str, config_name: str) -> dict | None:
    q = question.lower()
    has_returns_intent = ("возврат" in q) or ("возвращ" in q)
    if _normalize_config(config_name) != "ut":
        return None

    is_top5_revenue = (
        ("топ-5" in q or "top-5" in q or "топ 5" in q)
        and "клиент" in q
        and "выруч" in q
    )
    if is_top5_revenue:
        result = top_customers_by_revenue("ut", limit=5)
        return {
            "answer": result["markdown_table"],
            "tool_call": {
                "tool": "top_customers_by_revenue",
                "args": {"config_name": "ut", "date_from": "2024-01-01", "date_to": "2025-12-31", "limit": 5},
            },
        }

    is_top_products = ("топ" in q or "top" in q) and "товар" in q and "выруч" in q
    if is_top_products:
        result = top_products_by_revenue("ut", limit=10)
        return {
            "answer": result["markdown_table"],
            "tool_call": {
                "tool": "top_products_by_revenue",
                "args": {"config_name": "ut", "date_from": "2024-01-01", "date_to": "2025-12-31", "limit": 10},
            },
        }

    is_monthly_sales = ("по месяц" in q or "динамик" in q) and "продаж" in q
    if is_monthly_sales:
        result = monthly_sales_summary("ut", date_from="2024-01-01", date_to="2024-12-31")
        return {
            "answer": result["markdown_table"],
            "tool_call": {
                "tool": "monthly_sales_summary",
                "args": {"config_name": "ut", "date_from": "2024-01-01", "date_to": "2024-12-31"},
            },
        }

    is_returns_top = has_returns_intent and ("товар" in q or "продукт" in q) and (("чаще" in q) or ("топ" in q))
    if is_returns_top:
        result = top_returned_products("ut", date_from="2024-01-01", date_to="2024-12-31", limit=10)
        return {
            "answer": result["markdown_table"],
            "tool_call": {
                "tool": "top_returned_products",
                "args": {"config_name": "ut", "date_from": "2024-01-01", "date_to": "2024-12-31", "limit": 10},
            },
        }
    return None


def _build_options(config_name: str, model_name: str) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        system_prompt=_system_prompt(config_name),
        allowed_tools=ALLOWED_TOOLS,
        disallowed_tools=DISALLOWED_TOOLS,
        mcp_servers={"odata": _get_mcp_server()},
        can_use_tool=_build_tool_guard(config_name),
        permission_mode="bypassPermissions",
        max_turns=8,
        model=model_name,
        effort="low",
    )


async def _stream_with_model(
    question: str,
    config_name: str,
    model_name: str,
    emit,
    cancel_event=None,
) -> tuple[bool, list[str]]:
    """Stream a single model attempt. Returns (final_emitted, thinking_chunks)."""
    options = _build_options(config_name, model_name)
    final_emitted = False
    thinking_chunks = []
    first_chunk_seen = False
    timeout = FIRST_TOKEN_TIMEOUT_SEC if FIRST_TOKEN_TIMEOUT_SEC > 0 else None

    async with ClaudeSDKClient(options=options) as client:
        await client.query(question)
        stream = client.receive_response().__aiter__()
        while True:
            if cancel_event is not None and cancel_event.is_set():
                break
            try:
                if not first_chunk_seen and timeout:
                    message = await asyncio.wait_for(stream.__anext__(), timeout=timeout)
                else:
                    message = await stream.__anext__()
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                raise TimeoutError("first_token_timeout")

            first_chunk_seen = True
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    if isinstance(block, TextBlock) and block.text:
                        thinking_chunks.append(block.text)
                        emit({"type": "thinking", "text": block.text})
                    elif isinstance(block, ToolUseBlock):
                        name = block.name.replace("mcp__odata__", "")
                        emit({"type": "tool_call", "tool": name, "args": block.input})
            elif isinstance(message, ResultMessage) and message.result:
                final_emitted = True
                emit({"type": "final", "text": message.result})
    return final_emitted, thinking_chunks


def _ensure_warm_status_thread():
    global _warm_thread_started
    if _warm_thread_started:
        return
    _warm_thread_started = True

    def _warm():
        # Keep local runtime warm (MCP cache + auth check) without consuming model tokens.
        while True:
            try:
                _get_mcp_server()
                subprocess.run(["claude", "auth", "status"], capture_output=True, check=False, text=True)
            except Exception:
                pass
            time.sleep(max(30, WARM_STATUS_INTERVAL_SEC))

    threading.Thread(target=_warm, daemon=True).start()


async def _run(question: str, config_name: str) -> dict:
    fast = _try_direct_answer(question, config_name)
    if fast:
        return {"answer": fast["answer"], "tool_calls": [fast["tool_call"]]}

    tool_calls = []
    chunks = []
    events = stream_agent_events(question, config_name, timeout=120)
    for event in events:
        if event["type"] == "tool_call":
            tool_calls.append({"tool": event["tool"], "args": event["args"]})
        elif event["type"] == "thinking":
            chunks.append(event["text"])
        elif event["type"] == "final":
            chunks.append(event["text"])
    answer = "".join(chunks).strip() or "Нет ответа."
    return {"answer": answer, "tool_calls": tool_calls}


def stream_agent(question: str, config_name: str, tool_calls_out: list, timeout: int = 120, cancel_event=None):
    """Legacy text stream wrapper. Also captures tool calls via side effect."""
    seen_thinking = []
    for event in stream_agent_events(question, config_name, timeout=timeout, cancel_event=cancel_event):
        if event["type"] == "tool_call":
            tool_calls_out.append({"tool": event["tool"], "args": event["args"]})
        elif event["type"] == "thinking":
            seen_thinking.append(event["text"])
            yield event["text"]
        elif event["type"] == "final":
            text = event["text"]
            thinking_prefix = "".join(seen_thinking)
            if thinking_prefix and text.startswith(thinking_prefix):
                text = text[len(thinking_prefix):]
            if text:
                yield text


def stream_agent_events(question: str, config_name: str, timeout: int = 120, cancel_event=None):
    """Sync generator yielding typed events: thinking/tool_call/final."""
    q: queue.Queue = queue.Queue()
    thinking_chunks = []

    async def _stream():
        _ensure_warm_status_thread()
        q.put({"type": "status", "text": "🚀 Запрос отправлен агенту. Строю оптимальный план..."})
        fast = _try_direct_answer(question, config_name)
        if fast:
            q.put({"type": "status", "text": "⚡ Использую оптимизированный быстрый путь."})
            q.put({"type": "tool_call", "tool": fast["tool_call"]["tool"], "args": fast["tool_call"]["args"]})
            q.put({"type": "final", "text": fast["answer"]})
            return

        final_emitted = False
        attempt_models = [MODEL]
        if FALLBACK_MODEL and FALLBACK_MODEL != MODEL:
            attempt_models.append(FALLBACK_MODEL)

        for idx, model_name in enumerate(attempt_models, start=1):
            if cancel_event is not None and cancel_event.is_set():
                break
            q.put({"type": "status", "text": f"🧠 Модель: `{model_name}` (попытка {idx}/{len(attempt_models)})"})
            try:
                def _emit(event):
                    if event.get("type") == "thinking" and event.get("text"):
                        thinking_chunks.append(event["text"])
                    q.put(event)

                emitted, attempt_thinking = await _stream_with_model(
                    question=question,
                    config_name=config_name,
                    model_name=model_name,
                    emit=_emit,
                    cancel_event=cancel_event,
                )
                if attempt_thinking:
                    # Keep backward compatibility if inner streamer returns chunks too.
                    thinking_chunks.extend(attempt_thinking)
                if emitted:
                    final_emitted = True
                    break
            except TimeoutError:
                q.put(
                    {
                        "type": "status",
                        "text": f"⏱ Нет первого токена за {FIRST_TOKEN_TIMEOUT_SEC:.0f}с на `{model_name}`. Переключаю модель...",
                    }
                )
                continue

        if cancel_event is not None and cancel_event.is_set():
            partial = "".join(thinking_chunks).strip()
            cancel_text = "⛔ Операция отменена пользователем."
            if partial:
                q.put({"type": "final", "text": f"{partial}\n\n{cancel_text}"})
            else:
                q.put({"type": "final", "text": cancel_text})
        elif not final_emitted and thinking_chunks:
            q.put({"type": "final", "text": "".join(thinking_chunks)})
        elif not final_emitted:
            q.put({"type": "final", "text": "Нет ответа от модели. Попробуйте переформулировать запрос."})

    def _thread():
        async def _with_timeout():
            await asyncio.wait_for(_stream(), timeout=timeout)
        try:
            asyncio.run(_with_timeout())
        except asyncio.TimeoutError:
            partial = "".join(thinking_chunks).strip()
            if partial:
                q.put({"type": "final", "text": f"{partial}\n\n⏱ Агент не уложился в {timeout} секунд."})
            else:
                q.put({"type": "final", "text": f"\n⏱ Агент не уложился в {timeout} секунд."})
        except Exception as e:
            q.put({"type": "final", "text": f"\n❌ Ошибка: {e}"})
        finally:
            q.put({"type": "done"})

    threading.Thread(target=_thread, daemon=True).start()

    while True:
        event = q.get(timeout=timeout + 10)
        if event["type"] == "done":
            break
        yield event


def run_agent(question: str, config_name: str, timeout: int = 120) -> dict:
    """Non-streaming wrapper (used by tests)."""
    tool_calls: list = []
    chunks = list(stream_agent(question, config_name, tool_calls, timeout))
    answer = "".join(chunks) or "Нет ответа."
    return {"answer": answer, "tool_calls": tool_calls}
