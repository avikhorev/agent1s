from odata.client import fetch_by_key, fetch_entity, fetch_metadata, fetch_service_document, fetch_service_root
from odata.metadata import parse_entity_fields


def list_configs() -> list[str]:
    """List all available OData configurations (UT = Управление торговлей, BP = Бухгалтерия предприятия)."""
    payload = fetch_service_root()
    return [item["name"] for item in payload["configurations"]]


def list_entities(config_name: str) -> list[str]:
    """List all entity types in a configuration: catalogs (Catalog_*), documents (Document_*), registers (AccumulationRegister_*)."""
    payload = fetch_service_document(config_name)
    return [item["name"] for item in payload["value"]]


def describe_entity(config_name: str, entity_name: str) -> dict:
    """Get field names and types for an entity. Always call this before querying to know exact field names."""
    metadata_xml = fetch_metadata(config_name)
    description = parse_entity_fields(metadata_xml, entity_name)
    return {"entity": description.entity, "fields": description.fields}


def query_entity(
    config_name: str,
    entity_name: str,
    select: str = None,
    filter_expr: str = None,
    orderby: str = None,
    top: int = None,
    skip: int = None,
    count_only: bool = False,
) -> dict:
    """Query an OData entity collection. Use filter_expr for $filter (e.g. "Date ge datetime'2024-01-01'"), select for fields, orderby, top (max 50), skip, count_only."""
    payload = fetch_entity(
        config_name,
        entity_name,
        select=select,
        filter_expr=filter_expr,
        orderby=orderby,
        top=top,
        skip=skip,
        count_only=count_only,
    )
    records = payload.get("value", [])
    return {
        "config_name": config_name,
        "entity": entity_name,
        "records": records,
        "query": {
            "select": select,
            "filter": filter_expr,
            "orderby": orderby,
            "top": top,
            "skip": skip,
            "count_only": count_only,
        },
    }


def get_by_key(config_name: str, entity_name: str, ref_key: str) -> dict:
    """Fetch a single record by its Ref_Key (GUID)."""
    payload = fetch_by_key(config_name, entity_name, ref_key)
    return {"config_name": config_name, "entity": entity_name, "record": payload}
