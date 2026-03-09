import asyncio
import inspect
import json

from claude_agent_sdk import create_sdk_mcp_server
from claude_agent_sdk import tool as sdk_tool

import odata.tools as _tools

MAX_TOP = 50  # Hard cap to prevent SDK stdout buffer overflow


def _build_schema(fn) -> dict:
    sig = inspect.signature(fn)
    schema = {}
    for name, param in sig.parameters.items():
        ann = param.annotation
        if ann is inspect.Parameter.empty or ann is type(None):
            schema[name] = str
        elif ann in (int, float, str, bool):
            schema[name] = ann
        elif hasattr(ann, "__args__"):  # e.g. str | None
            inner = [a for a in ann.__args__ if a is not type(None)]
            schema[name] = inner[0] if inner else str
        else:
            schema[name] = str
    return schema


def _wrap(fn) -> object:
    schema = _build_schema(fn)

    async def wrapper(args: dict) -> dict:
        kwargs = {k: v for k, v in args.items() if v is not None}
        if "top" in schema:
            kwargs["top"] = min(int(kwargs["top"]), MAX_TOP) if kwargs.get("top") else MAX_TOP
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(None, lambda: fn(**kwargs))
            text = json.dumps(result, indent=2, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
            return {"content": [{"type": "text", "text": text}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Error: {e}"}]}

    return sdk_tool(fn.__name__, fn.__doc__ or fn.__name__, schema)(wrapper)


def create_odata_mcp_server():
    return create_sdk_mcp_server(
        name="odata-tools",
        tools=[_wrap(fn) for fn in (
            _tools.list_configs,
            _tools.list_entities,
            _tools.describe_entity,
            _tools.query_entity,
            _tools.get_by_key,
        )],
    )
