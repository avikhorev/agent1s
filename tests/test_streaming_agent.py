import types
import asyncio
import threading


def test_stream_agent_yields_text_and_tracks_tool_calls(monkeypatch):
    import agent

    class FakeTextBlock:
        def __init__(self, text: str):
            self.text = text

    class FakeToolUseBlock:
        def __init__(self, name: str, payload: dict):
            self.name = name
            self.input = payload
            self.id = "tool-1"

    class FakeAssistantMessage:
        def __init__(self, content):
            self.content = content

    class FakeResultMessage:
        def __init__(self, result: str):
            self.result = result

    class FakeClient:
        def __init__(self, options):
            self.options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, question):
            self.question = question

        async def receive_response(self):
            yield FakeAssistantMessage(
                [
                    FakeTextBlock("chunk-1"),
                    FakeToolUseBlock("mcp__odata__list_configs", {"config_name": "ut"}),
                ]
            )
            yield FakeResultMessage("chunk-2")

    monkeypatch.setattr(agent, "AssistantMessage", FakeAssistantMessage)
    monkeypatch.setattr(agent, "ResultMessage", FakeResultMessage)
    monkeypatch.setattr(agent, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(agent, "ToolUseBlock", FakeToolUseBlock)
    monkeypatch.setattr(agent, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(agent, "ClaudeAgentOptions", lambda **kwargs: types.SimpleNamespace(**kwargs))

    tool_calls = []
    chunks = list(agent.stream_agent("test question", "ut", tool_calls, timeout=2))

    assert chunks == ["chunk-1", "chunk-2"]
    assert len(tool_calls) == 1
    assert tool_calls[0]["tool"] == "list_configs"
    assert tool_calls[0]["args"] == {"config_name": "ut"}


def test_run_agent_aggregates_stream_chunks(monkeypatch):
    import agent

    monkeypatch.setattr(
        agent,
        "stream_agent",
        lambda question, config_name, tool_calls_out, timeout=120: iter(["a", "b", "c"]),
    )

    result = agent.run_agent("q", "ut", timeout=2)
    assert result == {"answer": "abc", "tool_calls": []}


def test_stream_agent_events_emits_thinking_tool_and_final(monkeypatch):
    import agent

    class FakeTextBlock:
        def __init__(self, text: str):
            self.text = text

    class FakeToolUseBlock:
        def __init__(self, name: str, payload: dict):
            self.name = name
            self.input = payload
            self.id = "tool-1"

    class FakeAssistantMessage:
        def __init__(self, content):
            self.content = content

    class FakeResultMessage:
        def __init__(self, result: str):
            self.result = result

    class FakeClient:
        def __init__(self, options):
            self.options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, question):
            self.question = question

        async def receive_response(self):
            yield FakeAssistantMessage(
                [
                    FakeTextBlock("think-1"),
                    FakeToolUseBlock("mcp__odata__describe_entity", {"config_name": "ut"}),
                    FakeTextBlock("think-2"),
                ]
            )
            yield FakeResultMessage("final-answer")

    monkeypatch.setattr(agent, "AssistantMessage", FakeAssistantMessage)
    monkeypatch.setattr(agent, "ResultMessage", FakeResultMessage)
    monkeypatch.setattr(agent, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(agent, "ToolUseBlock", FakeToolUseBlock)
    monkeypatch.setattr(agent, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(agent, "ClaudeAgentOptions", lambda **kwargs: types.SimpleNamespace(**kwargs))

    events = list(agent.stream_agent_events("q", "ut", timeout=2))
    assert events == [
        {"type": "thinking", "text": "think-1"},
        {"type": "tool_call", "tool": "describe_entity", "args": {"config_name": "ut"}},
        {"type": "thinking", "text": "think-2"},
        {"type": "final", "text": "final-answer"},
    ]


def test_stream_agent_legacy_wrapper_keeps_text_and_tool_calls(monkeypatch):
    import agent

    def fake_events(*args, **kwargs):
        yield {"type": "thinking", "text": "a"}
        yield {"type": "tool_call", "tool": "list_configs", "args": {"config_name": "ut"}}
        yield {"type": "final", "text": "b"}

    monkeypatch.setattr(agent, "stream_agent_events", fake_events)

    tool_calls = []
    chunks = list(agent.stream_agent("q", "ut", tool_calls, timeout=2))
    assert chunks == ["a", "b"]
    assert tool_calls == [{"tool": "list_configs", "args": {"config_name": "ut"}}]


def test_stream_agent_legacy_wrapper_deduplicates_timeout_final(monkeypatch):
    import agent

    def fake_events(*args, **kwargs):
        yield {"type": "thinking", "text": "partial"}
        yield {"type": "final", "text": "partial\n\n⏱ Агент не уложился в 2 секунд."}

    monkeypatch.setattr(agent, "stream_agent_events", fake_events)

    chunks = list(agent.stream_agent("q", "ut", [], timeout=2))
    assert chunks == ["partial", "\n\n⏱ Агент не уложился в 2 секунд."]


def test_stream_agent_events_timeout_returns_partial_as_final(monkeypatch):
    import agent

    class FakeTextBlock:
        def __init__(self, text: str):
            self.text = text

    class FakeAssistantMessage:
        def __init__(self, content):
            self.content = content

    class FakeClient:
        def __init__(self, options):
            self.options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, question):
            self.question = question

        async def receive_response(self):
            yield FakeAssistantMessage([FakeTextBlock("partial-answer")])
            await asyncio.sleep(1)

    monkeypatch.setattr(agent, "AssistantMessage", FakeAssistantMessage)
    monkeypatch.setattr(agent, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(agent, "ToolUseBlock", type("UnusedToolUseBlock", (), {}))
    monkeypatch.setattr(agent, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(agent, "ClaudeAgentOptions", lambda **kwargs: types.SimpleNamespace(**kwargs))

    events = list(agent.stream_agent_events("q", "ut", timeout=0.05))
    assert events[0] == {"type": "thinking", "text": "partial-answer"}
    assert events[1]["type"] == "final"
    assert "partial-answer" in events[1]["text"]
    assert "⏱ Агент не уложился" in events[1]["text"]


def test_stream_agent_events_cancel_emits_user_cancel_message(monkeypatch):
    import agent

    class FakeTextBlock:
        def __init__(self, text: str):
            self.text = text

    class FakeAssistantMessage:
        def __init__(self, content):
            self.content = content

    class FakeClient:
        def __init__(self, options):
            self.options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, question):
            self.question = question

        async def receive_response(self):
            yield FakeAssistantMessage([FakeTextBlock("partial-answer")])
            await asyncio.sleep(1)

    monkeypatch.setattr(agent, "AssistantMessage", FakeAssistantMessage)
    monkeypatch.setattr(agent, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(agent, "ToolUseBlock", type("UnusedToolUseBlock", (), {}))
    monkeypatch.setattr(agent, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(agent, "ClaudeAgentOptions", lambda **kwargs: types.SimpleNamespace(**kwargs))

    cancel_event = threading.Event()
    cancel_event.set()
    events = list(agent.stream_agent_events("q", "ut", timeout=2, cancel_event=cancel_event))
    assert events[-1]["type"] == "final"
    assert "Операция отменена пользователем" in events[-1]["text"]


def test_stream_agent_events_restricts_tools_to_odata(monkeypatch):
    import agent

    captured = {}

    class FakeTextBlock:
        def __init__(self, text: str):
            self.text = text

    class FakeAssistantMessage:
        def __init__(self, content):
            self.content = content

    class FakeResultMessage:
        def __init__(self, result: str):
            self.result = result

    class FakeClient:
        def __init__(self, options):
            captured["options"] = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, question):
            self.question = question

        async def receive_response(self):
            yield FakeAssistantMessage([FakeTextBlock("ok")])
            yield FakeResultMessage("done")

    monkeypatch.setattr(agent, "AssistantMessage", FakeAssistantMessage)
    monkeypatch.setattr(agent, "ResultMessage", FakeResultMessage)
    monkeypatch.setattr(agent, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(agent, "ToolUseBlock", type("UnusedToolUseBlock", (), {}))
    monkeypatch.setattr(agent, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(agent, "ClaudeAgentOptions", lambda **kwargs: types.SimpleNamespace(**kwargs))

    list(agent.stream_agent_events("q", "ut", timeout=2))

    opts = captured["options"]
    assert opts.allowed_tools == [
        "mcp__odata__list_configs",
        "mcp__odata__list_entities",
        "mcp__odata__describe_entity",
        "mcp__odata__query_entity",
        "mcp__odata__get_by_key",
    ]
    assert "Agent" in opts.disallowed_tools
    assert "TaskOutput" in opts.disallowed_tools
