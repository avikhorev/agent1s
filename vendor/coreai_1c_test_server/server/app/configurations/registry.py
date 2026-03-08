"""Global configuration registry."""
from app.configurations.trade import TRADE_CONFIG
from app.configurations.accounting import ACCOUNTING_CONFIG

CONFIGURATIONS: dict[str, dict] = {
    "ut": TRADE_CONFIG,
    "bp": ACCOUNTING_CONFIG,
}


def get_configuration(name: str) -> dict | None:
    return CONFIGURATIONS.get(name)


def get_entity_def(config_name: str, entity_name: str) -> dict | None:
    cfg = CONFIGURATIONS.get(config_name)
    if cfg is None:
        return None
    return cfg["entities"].get(entity_name)


def list_configurations() -> list[dict]:
    return [
        {"name": c["name"], "display_name": c["display_name"]}
        for c in CONFIGURATIONS.values()
    ]


def list_entities(config_name: str) -> list[str]:
    cfg = CONFIGURATIONS.get(config_name)
    if cfg is None:
        return []
    return list(cfg["entities"].keys())
