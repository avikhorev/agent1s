from unittest.mock import patch
import httpx
import respx

@respx.mock
def test_fetch_entity_builds_correct_url():
    route = respx.get(url__startswith="http://test/ut/odata/standard.odata/Catalog_Test").mock(
        return_value=httpx.Response(200, json={"value": [{"id": 1}]})
    )
    with patch("odata.client.ODATA_BASE_URL", "http://test"):
        from odata.client import fetch_entity
        result = fetch_entity("ut", "Catalog_Test", select="Description", filter_expr="IsFolder eq false", top=10, orderby="Description")

    assert route.called
    url = str(route.calls.last.request.url)
    assert "Catalog_Test" in url
    assert "%24select=Description" in url or "$select=Description" in url
    assert "%24top=10" in url or "$top=10" in url
    assert "%24orderby=Description" in url or "$orderby=Description" in url
    assert result["value"] == [{"id": 1}]


@respx.mock
def test_fetch_entity_count_only():
    route = respx.get(url__startswith="http://test/").mock(
        return_value=httpx.Response(200, json={"odata.count": "42"})
    )
    with patch("odata.client.ODATA_BASE_URL", "http://test"):
        from odata.client import fetch_entity
        fetch_entity("ut", "Catalog_Test", count_only=True)

    url = str(route.calls.last.request.url)
    assert "%24count=true" in url or "$count=true" in url


@respx.mock
def test_fetch_by_key_url():
    route = respx.get(url__startswith="http://test/").mock(
        return_value=httpx.Response(200, json={"Ref_Key": "abc-123"})
    )
    with patch("odata.client.ODATA_BASE_URL", "http://test"):
        from odata.client import fetch_by_key
        fetch_by_key("ut", "Catalog_Test", "abc-123")

    url = str(route.calls.last.request.url)
    assert "guid'abc-123'" in url


@respx.mock
def test_fetch_metadata_returns_text():
    respx.get("http://test/ut/odata/standard.odata/%24metadata").mock(
        return_value=httpx.Response(200, text="<xml>metadata</xml>")
    )
    with patch("odata.client.ODATA_BASE_URL", "http://test"):
        from odata.client import fetch_metadata
        result = fetch_metadata("ut")
    assert result == "<xml>metadata</xml>"
