from collections import defaultdict
from datetime import date, datetime, timedelta

from odata.client import fetch_by_key, fetch_entity, fetch_metadata, fetch_service_document, fetch_service_root
from odata.metadata import parse_entity_fields

HEAVY_ENTITIES = {"AccumulationRegister_Продажи"}
MAX_SKIP_NO_FILTER = 5000


def _default_dates() -> tuple[str, str]:
    """Last 12 months up to today."""
    today = date.today()
    return (today - timedelta(days=365)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _has_date_filter(filter_expr: str | None) -> bool:
    if not filter_expr:
        return False
    expr = filter_expr.lower()
    return ("period ge datetime'" in expr and "period le datetime'" in expr) or (
        "date ge datetime'" in expr and "date le datetime'" in expr
    )


def _is_aggregate_select(select: str | None) -> bool:
    if not select:
        return False
    s = select.lower()
    return "sum(" in s or "groupby(" in s or "aggregate(" in s


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
    optimization = {
        "plan": "optimized",
        "reason": "",
        "estimated_calls": 1,
        "guard_applied": False,
    }

    if entity_name in HEAVY_ENTITIES and not count_only:
        if not _has_date_filter(filter_expr) and not _is_aggregate_select(select):
            return {
                "config_name": config_name,
                "entity": entity_name,
                "records": [],
                "query": {
                    "select": select,
                    "filter": filter_expr,
                    "orderby": orderby,
                    "top": top,
                    "skip": skip,
                    "count_only": count_only,
                },
                "optimization": {
                    "plan": "rejected",
                    "reason": "missing_date_filter_for_heavy_entity",
                    "estimated_calls": "very_high",
                    "guard_applied": True,
                },
            }
        if (skip or 0) > MAX_SKIP_NO_FILTER and not _is_aggregate_select(select):
            return {
                "config_name": config_name,
                "entity": entity_name,
                "records": [],
                "query": {
                    "select": select,
                    "filter": filter_expr,
                    "orderby": orderby,
                    "top": top,
                    "skip": skip,
                    "count_only": count_only,
                },
                "optimization": {
                    "plan": "rejected",
                    "reason": "skip_too_large_for_heavy_entity",
                    "estimated_calls": "very_high",
                    "guard_applied": True,
                },
            }
        optimization["guard_applied"] = True

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
        "optimization": optimization,
    }


def get_by_key(config_name: str, entity_name: str, ref_key: str) -> dict:
    """Fetch a single record by its Ref_Key (GUID)."""
    payload = fetch_by_key(config_name, entity_name, ref_key)
    return {"config_name": config_name, "entity": entity_name, "record": payload}


def top_customers_by_revenue(
    config_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 10,
) -> dict:
    """Optimized server-side workflow for top-N customers by revenue. Scans AccumulationRegister_Продажи in large pages and returns ranked totals with customer names."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    page_size = 5000
    skip = 0
    chunks = 0
    rows_scanned = 0
    totals = defaultdict(float)

    filter_expr = f"Period ge datetime'{date_from}' and Period le datetime'{date_to}'"

    while True:
        payload = fetch_entity(
            config_name,
            "AccumulationRegister_Продажи",
            select="Контрагент_Key,Сумма",
            filter_expr=filter_expr,
            orderby="Контрагент_Key",
            top=page_size,
            skip=skip,
        )
        records = payload.get("value", [])
        if not records:
            break
        chunks += 1
        rows_scanned += len(records)
        for row in records:
            key = row.get("Контрагент_Key")
            if not key:
                continue
            try:
                totals[key] += float(row.get("Сумма") or 0)
            except Exception:
                continue
        if len(records) < page_size:
            break
        skip += page_size

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[: max(1, int(limit))]

    table = []
    for idx, (ref_key, total) in enumerate(ranked, start=1):
        try:
            customer = fetch_by_key(config_name, "Catalog_Контрагенты", ref_key)
            name = customer.get("Description") or ref_key
        except Exception:
            name = ref_key
        table.append(
            {
                "rank": idx,
                "customer_ref_key": ref_key,
                "customer_name": name,
                "total_revenue": round(total, 2),
            }
        )

    md_lines = ["| Rank | CustomerName | TotalRevenue |", "|---:|---|---:|"]
    for row in table:
        md_lines.append(f"| {row['rank']} | {row['customer_name']} | {row['total_revenue']:,.2f} |")

    return {
        "config_name": config_name,
        "date_from": date_from,
        "date_to": date_to,
        "rows_scanned": rows_scanned,
        "chunks": chunks,
        "table": table,
        "markdown_table": "\n".join(md_lines),
        "optimization": {
            "plan": "optimized",
            "reason": "specialized_top_customers_path",
            "estimated_calls": chunks + len(table),
            "guard_applied": True,
        },
    }


def top_products_by_revenue(
    config_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 10,
) -> dict:
    """Optimized top-N products by revenue in UT using AccumulationRegister_Продажи."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    page_size = 5000
    skip = 0
    chunks = 0
    rows_scanned = 0
    totals = defaultdict(float)

    filter_expr = f"Period ge datetime'{date_from}' and Period le datetime'{date_to}'"
    while True:
        payload = fetch_entity(
            config_name,
            "AccumulationRegister_Продажи",
            select="Номенклатура_Key,Сумма",
            filter_expr=filter_expr,
            orderby="Номенклатура_Key",
            top=page_size,
            skip=skip,
        )
        records = payload.get("value", [])
        if not records:
            break
        chunks += 1
        rows_scanned += len(records)
        for row in records:
            key = row.get("Номенклатура_Key")
            if not key:
                continue
            try:
                totals[key] += float(row.get("Сумма") or 0)
            except Exception:
                continue
        if len(records) < page_size:
            break
        skip += page_size

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[: max(1, int(limit))]
    table = []
    for idx, (ref_key, total) in enumerate(ranked, start=1):
        try:
            item = fetch_by_key(config_name, "Catalog_Номенклатура", ref_key)
            name = item.get("Description") or ref_key
        except Exception:
            name = ref_key
        table.append(
            {
                "rank": idx,
                "product_ref_key": ref_key,
                "product_name": name,
                "total_revenue": round(total, 2),
            }
        )

    md_lines = ["| Rank | Product | TotalRevenue |", "|---:|---|---:|"]
    for row in table:
        md_lines.append(f"| {row['rank']} | {row['product_name']} | {row['total_revenue']:,.2f} |")

    return {
        "config_name": config_name,
        "date_from": date_from,
        "date_to": date_to,
        "rows_scanned": rows_scanned,
        "chunks": chunks,
        "table": table,
        "markdown_table": "\n".join(md_lines),
        "optimization": {
            "plan": "optimized",
            "reason": "specialized_top_products_path",
            "estimated_calls": chunks + len(table),
            "guard_applied": True,
        },
    }


def monthly_sales_summary(
    config_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Optimized monthly sales summary from AccumulationRegister_Продажи."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    page_size = 5000
    skip = 0
    chunks = 0
    rows_scanned = 0
    totals = defaultdict(float)

    filter_expr = f"Period ge datetime'{date_from}' and Period le datetime'{date_to}'"
    while True:
        payload = fetch_entity(
            config_name,
            "AccumulationRegister_Продажи",
            select="Period,Сумма",
            filter_expr=filter_expr,
            orderby="Period",
            top=page_size,
            skip=skip,
        )
        records = payload.get("value", [])
        if not records:
            break
        chunks += 1
        rows_scanned += len(records)
        for row in records:
            period = row.get("Period")
            if not period:
                continue
            try:
                month = datetime.fromisoformat(str(period).replace("Z", "+00:00")).strftime("%Y-%m")
                totals[month] += float(row.get("Сумма") or 0)
            except Exception:
                continue
        if len(records) < page_size:
            break
        skip += page_size

    months = sorted(totals.items(), key=lambda kv: kv[0])
    md_lines = ["| Month | Sales |", "|---|---:|"]
    for month, total in months:
        md_lines.append(f"| {month} | {round(total, 2):,.2f} |")

    return {
        "config_name": config_name,
        "date_from": date_from,
        "date_to": date_to,
        "rows_scanned": rows_scanned,
        "chunks": chunks,
        "table": [{"month": m, "sales": round(v, 2)} for m, v in months],
        "markdown_table": "\n".join(md_lines),
        "optimization": {
            "plan": "optimized",
            "reason": "specialized_monthly_sales_path",
            "estimated_calls": chunks,
            "guard_applied": True,
        },
    }


def top_returned_products(
    config_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 10,
) -> dict:
    """Optimized top-N returned products for UT from Document_ВозвратТоваровОтКлиента_Товары."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    page_size = 2000

    # Step 1: collect Ref_Keys of return documents within the date range.
    doc_filter = f"Posted eq true and Date ge datetime'{date_from}' and Date le datetime'{date_to}'"
    valid_ref_keys: set[str] = set()
    skip = 0
    while True:
        payload = fetch_entity(
            config_name,
            "Document_ВозвратТоваровОтКлиента",
            select="Ref_Key",
            filter_expr=doc_filter,
            top=page_size,
            skip=skip,
        )
        records = payload.get("value", [])
        if not records:
            break
        for row in records:
            key = row.get("Ref_Key")
            if key:
                valid_ref_keys.add(key)
        if len(records) < page_size:
            break
        skip += page_size

    # Step 2: scan line items, keeping only those from matched documents.
    skip = 0
    chunks = 0
    rows_scanned = 0
    totals = defaultdict(float)

    while True:
        payload = fetch_entity(
            config_name,
            "Document_ВозвратТоваровОтКлиента_Товары",
            select="Ref_Key,Номенклатура_Key,Сумма",
            top=page_size,
            skip=skip,
        )
        records = payload.get("value", [])
        if not records:
            break
        chunks += 1
        rows_scanned += len(records)
        for row in records:
            if row.get("Ref_Key") not in valid_ref_keys:
                continue
            key = row.get("Номенклатура_Key")
            if not key:
                continue
            try:
                totals[key] += float(row.get("Сумма") or 0)
            except Exception:
                continue
        if len(records) < page_size:
            break
        skip += page_size

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[: max(1, int(limit))]
    table = []
    for idx, (ref_key, total) in enumerate(ranked, start=1):
        try:
            item = fetch_by_key(config_name, "Catalog_Номенклатура", ref_key)
            name = item.get("Description") or ref_key
        except Exception:
            name = ref_key
        table.append(
            {
                "rank": idx,
                "product_ref_key": ref_key,
                "product_name": name,
                "return_amount": round(total, 2),
            }
        )

    md_lines = ["| Rank | Product | ReturnAmount |", "|---:|---|---:|"]
    for row in table:
        md_lines.append(f"| {row['rank']} | {row['product_name']} | {row['return_amount']:,.2f} |")

    return {
        "config_name": config_name,
        "date_from": date_from,
        "date_to": date_to,
        "rows_scanned": rows_scanned,
        "chunks": chunks,
        "table": table,
        "markdown_table": "\n".join(md_lines),
        "optimization": {
            "plan": "optimized",
            "reason": "specialized_top_returns_path",
            "estimated_calls": chunks + len(table),
            "guard_applied": True,
        },
    }
