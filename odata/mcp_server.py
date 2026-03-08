from claude_agent_sdk import create_sdk_mcp_server

from odata.mcp_tools import (
    describe_entity_tool,
    get_by_key_tool,
    list_configs_tool,
    list_entities_tool,
    query_entity_tool,
)


def create_odata_mcp_server():
    return create_sdk_mcp_server(
        name="odata-tools",
        tools=[
            list_configs_tool,
            list_entities_tool,
            describe_entity_tool,
            query_entity_tool,
            get_by_key_tool,
        ]
    )
