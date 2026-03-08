from odata.client import fetch_by_key, fetch_entity, fetch_metadata, fetch_service_document, fetch_service_root
from odata.metadata import parse_entity_fields


def list_configs() -> list[str]:
    payload = fetch_service_root()
    return [item["name"] for item in payload["configurations"]]


def list_entities(config_name: str) -> list[str]:
    payload = fetch_service_document(config_name)
    return [item["name"] for item in payload["value"]]


def describe_entity(config_name: str, entity_name: str) -> dict:
    metadata_xml = fetch_metadata(config_name)
    description = parse_entity_fields(metadata_xml, entity_name)
    return {"entity": description.entity, "fields": description.fields}


def query_entity(
    config_name: str,
    entity_name: str,
    *,
    select: str | None = None,
    filter_expr: str | None = None,
    orderby: str | None = None,
    top: int | None = None,
    skip: int | None = None,
    count_only: bool = False,
) -> dict:
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
    payload = fetch_by_key(config_name, entity_name, ref_key)
    return {"config_name": config_name, "entity": entity_name, "record": payload}
