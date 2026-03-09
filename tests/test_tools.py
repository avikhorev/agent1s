from unittest.mock import patch


def test_list_configs():
    with patch("odata.tools.fetch_service_root", return_value={"configurations": [{"name": "ut"}, {"name": "bp"}]}):
        from odata.tools import list_configs
        assert list_configs() == ["ut", "bp"]


def test_list_entities():
    with patch("odata.tools.fetch_service_document", return_value={"value": [{"name": "Catalog_A"}, {"name": "Document_B"}]}):
        from odata.tools import list_entities
        assert list_entities("ut") == ["Catalog_A", "Document_B"]


def test_query_entity_returns_structured_dict():
    mock_response = {"value": [{"Ref_Key": "abc", "Description": "Test"}]}
    with patch("odata.tools.fetch_entity", return_value=mock_response):
        from odata.tools import query_entity
        result = query_entity("ut", "Catalog_Test", top=5, filter_expr="x eq 1")

    assert result["entity"] == "Catalog_Test"
    assert result["config_name"] == "ut"
    assert len(result["records"]) == 1
    assert result["query"]["top"] == 5
    assert result["query"]["filter"] == "x eq 1"


def test_get_by_key():
    mock_record = {"Ref_Key": "abc", "Description": "Item"}
    with patch("odata.tools.fetch_by_key", return_value=mock_record):
        from odata.tools import get_by_key
        result = get_by_key("ut", "Catalog_Test", "abc")

    assert result["record"] == mock_record
    assert result["entity"] == "Catalog_Test"
