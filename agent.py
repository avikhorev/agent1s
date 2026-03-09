"""Claude Agent SDK runner for 1C OData queries."""
import asyncio
import os
import queue
import threading

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

_mcp_server = None


def _get_mcp_server():
    global _mcp_server
    if _mcp_server is None:
        _mcp_server = create_odata_mcp_server()
    return _mcp_server


MODEL = os.getenv("ANTHROPIC_MODEL", "openrouter/free")
ALLOWED_TOOLS = [
    "mcp__odata__list_configs",
    "mcp__odata__list_entities",
    "mcp__odata__describe_entity",
    "mcp__odata__query_entity",
    "mcp__odata__get_by_key",
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
    config_display = {
        "ut": "Управление торговлей 11 (торговый учёт: номенклатура, продажи, закупки, возвраты, склады)",
        "bp": "Бухгалтерия предприятия 3.0 (бухучёт: реализации, поступления, платежи, взаиморасчёты)",
    }.get(config_name, config_name)

    return f"""Ты — аналитический AI-агент для работы с данными 1С:Предприятие через OData API.
Текущая конфигурация: **{config_display}**

## Сущности конфигурации UT (Управление торговлей)

**Справочники (Catalog_*):**
- Catalog_Номенклатура — товары. Поля: Description, Артикул, ЕдиницаИзмерения, Производитель, IsFolder
- Catalog_Контрагенты — поставщики и клиенты. Поля: Description, ЭтоПоставщик, ЭтоКлиент, ИНН
- Catalog_Склады — склады и магазины. Поля: Description, Город, ТипСклада
- Catalog_Организации — юр. лица. Поля: Description, ИНН
- Catalog_ВидыЦен — виды цен (Розничная, Оптовая, Закупочная)

**Документы (Document_*):**
- Document_РеализацияТоваровУслуг — продажи (~8000 за 2 года). Поля: Date, Number, Сумма, Контрагент_Key, Склад_Key
- Document_РеализацияТоваровУслуг_Товары — строки продаж. Поля: Ref_Key (=документ), Номенклатура_Key, Количество, Цена, Сумма
- Document_ПоступлениеТоваровУслуг — закупки (~300). Поля: Date, Сумма, Контрагент_Key
- Document_ВозвратТоваровОтКлиента — возвраты (~100+). Поля: Date, Сумма, Контрагент_Key
- Document_ЗаказКлиента — заказы клиентов

**Регистры накопления (AccumulationRegister_*):**
- AccumulationRegister_Продажи — движения по продажам (~55000 записей). Поля: Period, Номенклатура_Key, Контрагент_Key, Склад_Key, Количество, Сумма, Стоимость
- AccumulationRegister_Закупки — движения по закупкам
- AccumulationRegister_ТоварыНаСкладах — остатки товаров. Поля: Номенклатура_Key, Склад_Key, Количество, ВидДвижения

**Регистр сведений (InformationRegister_*):**
- InformationRegister_ЦеныНоменклатуры — цены. Поля: Period, Номенклатура_Key, ВидЦен_Key, Цена

## Сущности конфигурации BP (Бухгалтерия)

**Справочники:** Catalog_Номенклатура, Catalog_Контрагенты, Catalog_Организации, Catalog_СтатьиЗатрат, Catalog_Подразделения

**Документы:** Document_РеализацияТоваровУслуг, Document_ПоступлениеТоваровУслуг, Document_ПлатежноеПоручение (Сумма, НазначениеПлатежа), Document_ПоступлениеНаРасчетныйСчет

**Регистр:** AccumulationRegister_ВзаиморасчетыСКонтрагентами — Контрагент_Key, Сумма, ВидДвижения

## Как работать с данными

1. **Используй describe_entity** перед запросом чтобы узнать точные имена полей
2. **Фильтры по дате:** `Date ge datetime'2024-01-01' and Date le datetime'2024-12-31'`
3. **Связи через _Key:** Получи Ref_Key из одной сущности, используй как фильтр в другой
4. **Для аналитики:** Используй AccumulationRegister_Продажи (есть Сумма и Стоимость — можно считать маржу)
5. **Ограничивай выборку:** Максимум top=50 на запрос (жёсткий лимит). Используй $filter и $select чтобы получить только нужные поля и записи. Никогда не запрашивай всё подряд.
6. **Сезонность заложена в данных:** пик в ноябре-декабре, спад в январе-феврале

## Формат ответа

- Отвечай на русском языке
- Давай конкретные цифры из данных
- Форматируй таблицы в markdown
- Объясняй выводы, не просто перечисляй данные
- Если нашёл аномалию — объясни её значимость
"""


async def _run(question: str, config_name: str) -> dict:
    options = ClaudeAgentOptions(
        system_prompt=_system_prompt(config_name),
        allowed_tools=ALLOWED_TOOLS,
        disallowed_tools=DISALLOWED_TOOLS,
        mcp_servers={"odata": _get_mcp_server()},
        can_use_tool=_build_tool_guard(config_name),
        permission_mode="bypassPermissions",
        max_turns=15,
        model=MODEL,
    )

    text_blocks = []
    result_text = ""
    tool_calls = []

    async with ClaudeSDKClient(options=options) as client:
        await client.query(question)
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock) and block.text:
                        text_blocks.append(block.text)
                    elif isinstance(block, ToolUseBlock):
                        name = block.name.replace("mcp__odata__", "")
                        tool_calls.append({"tool": name, "args": block.input})
            elif isinstance(message, ResultMessage) and message.result:
                result_text = message.result

    answer = result_text or "\n".join(text_blocks) or "Нет ответа."
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
        final_emitted = False
        options = ClaudeAgentOptions(
            system_prompt=_system_prompt(config_name),
            allowed_tools=ALLOWED_TOOLS,
            disallowed_tools=DISALLOWED_TOOLS,
            mcp_servers={"odata": _get_mcp_server()},
            can_use_tool=_build_tool_guard(config_name),
            permission_mode="bypassPermissions",
            max_turns=15,
            model=MODEL,
        )
        async with ClaudeSDKClient(options=options) as client:
            await client.query(question)
            async for message in client.receive_response():
                if cancel_event is not None and cancel_event.is_set():
                    break
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if cancel_event is not None and cancel_event.is_set():
                            break
                        if isinstance(block, TextBlock) and block.text:
                            thinking_chunks.append(block.text)
                            q.put({"type": "thinking", "text": block.text})
                        elif isinstance(block, ToolUseBlock):
                            name = block.name.replace("mcp__odata__", "")
                            q.put({"type": "tool_call", "tool": name, "args": block.input})
                elif isinstance(message, ResultMessage) and message.result:
                    final_emitted = True
                    q.put({"type": "final", "text": message.result})

        if cancel_event is not None and cancel_event.is_set():
            partial = "".join(thinking_chunks).strip()
            cancel_text = "⛔ Операция отменена пользователем."
            if partial:
                q.put({"type": "final", "text": f"{partial}\n\n{cancel_text}"})
            else:
                q.put({"type": "final", "text": cancel_text})
        elif not final_emitted and thinking_chunks:
            q.put({"type": "final", "text": "".join(thinking_chunks)})

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
