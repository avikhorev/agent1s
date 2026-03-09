"""Claude Agent SDK runner for 1C OData queries."""
import asyncio
import os
import queue
import threading

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
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


MODEL = os.getenv("ANTHROPIC_DEFAULT_HAIKU_MODEL", "stepfun/step-3.5-flash:free")


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
        allowed_tools=[],
        mcp_servers={"odata": _get_mcp_server()},
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


def stream_agent(question: str, config_name: str, tool_calls_out: list, timeout: int = 120):
    """Sync generator yielding text chunks as Claude responds. Populates tool_calls_out as side effect."""
    q: queue.Queue = queue.Queue()

    async def _stream():
        options = ClaudeAgentOptions(
            system_prompt=_system_prompt(config_name),
            allowed_tools=[],
            mcp_servers={"odata": _get_mcp_server()},
            permission_mode="bypassPermissions",
            max_turns=15,
            model=MODEL,
        )
        async with ClaudeSDKClient(options=options) as client:
            await client.query(question)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock) and block.text:
                            q.put(("text", block.text))
                        elif isinstance(block, ToolUseBlock):
                            name = block.name.replace("mcp__odata__", "")
                            tool_calls_out.append({"tool": name, "args": block.input})
                elif isinstance(message, ResultMessage) and message.result:
                    q.put(("text", message.result))

    def _thread():
        async def _with_timeout():
            await asyncio.wait_for(_stream(), timeout=timeout)
        try:
            asyncio.run(_with_timeout())
        except asyncio.TimeoutError:
            q.put(("text", f"\n⏱ Агент не уложился в {timeout} секунд."))
        except Exception as e:
            q.put(("text", f"\n❌ Ошибка: {e}"))
        finally:
            q.put(("done", None))

    threading.Thread(target=_thread, daemon=True).start()

    while True:
        kind, value = q.get(timeout=timeout + 10)
        if kind == "done":
            break
        yield value


def run_agent(question: str, config_name: str, timeout: int = 120) -> dict:
    """Non-streaming wrapper (used by tests)."""
    tool_calls: list = []
    chunks = list(stream_agent(question, config_name, tool_calls, timeout))
    answer = "".join(chunks) or "Нет ответа."
    return {"answer": answer, "tool_calls": tool_calls}
