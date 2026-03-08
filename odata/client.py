import os
from urllib.parse import urlencode

import httpx

ODATA_BASE_URL = os.getenv("ODATA_MOCK_URL", "http://localhost:8080")


def _client() -> httpx.Client:
    return httpx.Client(base_url=ODATA_BASE_URL, timeout=10.0, trust_env=False)


def fetch_service_root() -> dict:
    with _client() as client:
        response = client.get("/")
        response.raise_for_status()
        return response.json()


def fetch_service_document(config_name: str) -> dict:
    with _client() as client:
        response = client.get(f"/{config_name}/odata/standard.odata")
        response.raise_for_status()
        return response.json()


def fetch_metadata(config_name: str) -> str:
    with _client() as client:
        response = client.get(f"/{config_name}/odata/standard.odata/$metadata")
        response.raise_for_status()
        return response.text


def fetch_entity(
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
    params: dict[str, str | int] = {"$format": "json"}
    if select:
        params["$select"] = select
    if filter_expr:
        params["$filter"] = filter_expr
    if orderby:
        params["$orderby"] = orderby
    if top is not None:
        params["$top"] = top
    if skip is not None:
        params["$skip"] = skip
    if count_only:
        params["$count"] = "true"

    query = urlencode(params)
    with _client() as client:
        response = client.get(f"/{config_name}/odata/standard.odata/{entity_name}?{query}")
        response.raise_for_status()
        return response.json()


def fetch_by_key(config_name: str, entity_name: str, ref_key: str) -> dict:
    with _client() as client:
        response = client.get(
            f"/{config_name}/odata/standard.odata/{entity_name}(guid'{ref_key}')?$format=json"
        )
        response.raise_for_status()
        return response.json()
