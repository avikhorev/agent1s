"""Format SQLAlchemy result rows as OData v3 JSON responses."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID


def _sv(val):
    if val is None:
        return None
    if isinstance(val, UUID):
        return str(val)
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bool):
        return val
    return val


def serialize_row(row, columns=None):
    rd = row._mapping if hasattr(row, "_mapping") else dict(row)
    return {str(k): _sv(v) for k, v in rd.items() if not columns or k in columns}


def serialize_collection(rows, metadata_url, columns=None):
    return {
        "odata.metadata": metadata_url,
        "value": [serialize_row(r, columns) for r in rows],
    }


def serialize_entity(row, metadata_url):
    result = {"odata.metadata": metadata_url}
    result.update(serialize_row(row))
    return result


def serialize_count(count):
    return str(count)


def serialize_error(message, code="BadRequest"):
    return {
        "odata.error": {
            "code": code,
            "message": {"lang": "ru", "value": message},
        }
    }
