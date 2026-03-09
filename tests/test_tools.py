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


def test_query_entity_rejects_unbounded_heavy_scan():
    from odata.tools import query_entity

    result = query_entity("ut", "AccumulationRegister_Продажи", select="Контрагент_Key,Сумма")
    assert result["records"] == []
    assert result["optimization"]["plan"] == "rejected"
    assert result["optimization"]["reason"] == "missing_date_filter_for_heavy_entity"


def test_top_customers_by_revenue_aggregates_and_resolves_names():
    from odata.tools import top_customers_by_revenue

    only_page = {
        "value": [
            {"Контрагент_Key": "k1", "Сумма": 100},
            {"Контрагент_Key": "k2", "Сумма": 80},
            {"Контрагент_Key": "k3", "Сумма": 20},
        ]
    }

    with patch("odata.tools.fetch_entity", return_value=only_page), patch(
        "odata.tools.fetch_by_key",
        side_effect=[
            {"Description": "Client 1"},
            {"Description": "Client 2"},
        ],
    ):
        result = top_customers_by_revenue("ut", limit=2)

    assert result["optimization"]["plan"] == "optimized"
    assert result["rows_scanned"] == 3
    assert result["chunks"] == 1
    assert result["table"][0]["customer_name"] == "Client 1"
    assert result["table"][0]["total_revenue"] == 100.0
    assert result["table"][1]["customer_name"] == "Client 2"


def test_top_products_by_revenue_aggregates_and_resolves_names():
    from odata.tools import top_products_by_revenue

    only_page = {
        "value": [
            {"Номенклатура_Key": "p1", "Сумма": 110},
            {"Номенклатура_Key": "p2", "Сумма": 80},
            {"Номенклатура_Key": "p1", "Сумма": 10},
        ]
    }

    with patch("odata.tools.fetch_entity", return_value=only_page), patch(
        "odata.tools.fetch_by_key",
        side_effect=[{"Description": "Prod 1"}, {"Description": "Prod 2"}],
    ):
        result = top_products_by_revenue("ut", limit=2)

    assert result["optimization"]["plan"] == "optimized"
    assert result["rows_scanned"] == 3
    assert result["table"][0]["product_name"] == "Prod 1"
    assert result["table"][0]["total_revenue"] == 120.0


def test_monthly_sales_summary_aggregates_by_month():
    from odata.tools import monthly_sales_summary

    only_page = {
        "value": [
            {"Period": "2024-01-15T00:00:00", "Сумма": 100},
            {"Period": "2024-01-20T00:00:00", "Сумма": 20},
            {"Period": "2024-02-01T00:00:00", "Сумма": 50},
        ]
    }
    with patch("odata.tools.fetch_entity", return_value=only_page):
        result = monthly_sales_summary("ut")

    assert result["optimization"]["plan"] == "optimized"
    assert result["table"][0]["month"] == "2024-01"
    assert result["table"][0]["sales"] == 120.0
    assert result["table"][1]["month"] == "2024-02"


def test_top_returned_products_aggregates_and_resolves_names():
    from odata.tools import top_returned_products

    # Step 1: header docs with Ref_Keys; Step 2: line items referencing those keys.
    headers_page = {"value": [{"Ref_Key": "doc1"}, {"Ref_Key": "doc2"}]}
    lines_page = {
        "value": [
            {"Ref_Key": "doc1", "Номенклатура_Key": "r1", "Сумма": 40},
            {"Ref_Key": "doc2", "Номенклатура_Key": "r2", "Сумма": 30},
            {"Ref_Key": "doc1", "Номенклатура_Key": "r1", "Сумма": 5},
        ]
    }
    with patch("odata.tools.fetch_entity", side_effect=[headers_page, lines_page]), patch(
        "odata.tools.fetch_by_key",
        side_effect=[{"Description": "Ret 1"}, {"Description": "Ret 2"}],
    ):
        result = top_returned_products("ut", limit=2)

    assert result["optimization"]["plan"] == "optimized"
    assert result["table"][0]["product_name"] == "Ret 1"
    assert result["table"][0]["return_amount"] == 45.0
