"""MCP-compatible tool wrappers for OData operations."""
import asyncio
import json

from claude_agent_sdk import tool as sdk_tool

MAX_TOP = 50  # Hard cap to prevent stdout buffer overflow in claude subprocess


@sdk_tool(
    name="list_configs",
    description="List all available OData configurations (UT = Управление торговлей, BP = Бухгалтерия предприятия)",
    input_schema={}
)
async def list_configs_tool(args: dict) -> dict:
    loop = asyncio.get_running_loop()
    try:
        from odata.tools import list_configs
        result = await loop.run_in_executor(None, list_configs)
        return {"content": [{"type": "text", "text": json.dumps({"configurations": result}, indent=2)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error: {e}"}]}


@sdk_tool(
    name="list_entities",
    description="List all entity types in a specific OData configuration (catalogs, documents, registers)",
    input_schema={"config_name": str}
)
async def list_entities_tool(args: dict) -> dict:
    loop = asyncio.get_running_loop()
    try:
        from odata.tools import list_entities
        config_name = args["config_name"]
        result = await loop.run_in_executor(None, lambda: list_entities(config_name))
        return {"content": [{"type": "text", "text": json.dumps({"config": config_name, "entities": result}, indent=2)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error: {e}"}]}


@sdk_tool(
    name="describe_entity",
    description="Get metadata for an entity: field names, types, keys. Always call this before querying to know field names.",
    input_schema={"config_name": str, "entity_name": str}
)
async def describe_entity_tool(args: dict) -> dict:
    loop = asyncio.get_running_loop()
    try:
        from odata.tools import describe_entity
        result = await loop.run_in_executor(
            None, lambda: describe_entity(args["config_name"], args["entity_name"])
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, ensure_ascii=False)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error: {e}"}]}


@sdk_tool(
    name="query_entity",
    description=(
        "Query an OData entity collection. Supports $filter, $select, $orderby, $top, $skip, $count. "
        "Use filter_expr for $filter expressions. "
        "Date filter example: Date ge datetime'2024-01-01' and Date le datetime'2024-12-31'. "
        "String filter example: substringof('текст', Description). "
        "Boolean filter example: IsFolder eq false."
    ),
    input_schema={
        "config_name": str,
        "entity_name": str,
        "select": str,
        "filter": str,
        "orderby": str,
        "top": int,
        "skip": int,
        "count_only": bool,
    }
)
async def query_entity_tool(args: dict) -> dict:
    loop = asyncio.get_running_loop()
    try:
        from odata.tools import query_entity
        requested_top = args.get("top")
        capped_top = min(requested_top, MAX_TOP) if requested_top else MAX_TOP
        result = await loop.run_in_executor(
            None,
            lambda: query_entity(
                args["config_name"],
                args["entity_name"],
                select=args.get("select"),
                filter_expr=args.get("filter"),
                orderby=args.get("orderby"),
                top=capped_top,
                skip=args.get("skip"),
                count_only=args.get("count_only", False),
            )
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, ensure_ascii=False)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Query failed: {e}\nCheck entity name and filter syntax."}]}


@sdk_tool(
    name="get_by_key",
    description="Retrieve a single entity record by its Ref_Key (GUID)",
    input_schema={"config_name": str, "entity_name": str, "ref_key": str}
)
async def get_by_key_tool(args: dict) -> dict:
    loop = asyncio.get_running_loop()
    try:
        from odata.tools import get_by_key
        result = await loop.run_in_executor(
            None, lambda: get_by_key(args["config_name"], args["entity_name"], args["ref_key"])
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, ensure_ascii=False)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error: {e}"}]}
