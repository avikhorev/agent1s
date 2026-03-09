import types


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

