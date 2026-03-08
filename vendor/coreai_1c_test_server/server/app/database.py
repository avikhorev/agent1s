from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import (
    MetaData, Table, Column, String, Boolean, DateTime,
    Integer, Float, Numeric, PrimaryKeyConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

from app.config import settings

engine = create_async_engine(settings.database_url, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)
metadata = MetaData()

EDM_TO_SA_TYPE = {
    "Edm.Guid": lambda f: UUID(as_uuid=True),
    "Edm.String": lambda f: String(f.get("max_length", 255)),
    "Edm.Boolean": lambda f: Boolean(),
    "Edm.DateTime": lambda f: DateTime(),
    "Edm.Int32": lambda f: Integer(),
    "Edm.Int64": lambda f: Integer(),
    "Edm.Decimal": lambda f: Numeric(15, 2),
    "Edm.Double": lambda f: Float(),
}


def build_table(entity_def: dict, meta: MetaData) -> Table:
    columns = []
    pk_cols = []
    for field in entity_def["fields"]:
        sa_type = EDM_TO_SA_TYPE[field["edm_type"]](field)
        is_pk = field.get("primary_key", False)
        col = Column(field["name"], sa_type, nullable=field.get("nullable", not is_pk))
        columns.append(col)
        if is_pk:
            pk_cols.append(field["name"])

    table = Table(entity_def["table_name"], meta, *columns)
    if len(pk_cols) > 1:
        table.append_constraint(PrimaryKeyConstraint(*pk_cols))
    return table


_tables: dict[str, dict[str, Table]] = {}


def get_tables() -> dict[str, dict[str, Table]]:
    return _tables


def init_tables(configurations: dict) -> dict[str, dict[str, Table]]:
    for config_name, config in configurations.items():
        _tables[config_name] = {}
        for entity_name, entity_def in config["entities"].items():
            _tables[config_name][entity_name] = build_table(entity_def, metadata)
    return _tables
