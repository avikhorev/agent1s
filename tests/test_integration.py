import pytest

pytestmark = pytest.mark.integration


def test_service_root():
    from odata.client import fetch_service_root
    data = fetch_service_root()
    configs = [c["name"] for c in data["configurations"]]
    assert "ut" in configs
    assert "bp" in configs


def test_list_entities_ut():
    from odata.tools import list_entities
    entities = list_entities("ut")
    assert "Catalog_Номенклатура" in entities
    assert "Document_РеализацияТоваровУслуг" in entities


def test_query_with_top():
    from odata.tools import query_entity
    result = query_entity("ut", "Catalog_Номенклатура", top=3)
    assert len(result["records"]) == 3
    assert "Ref_Key" in result["records"][0]
