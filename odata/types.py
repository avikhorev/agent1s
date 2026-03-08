from dataclasses import dataclass


@dataclass
class EntityDescription:
    entity: str
    fields: dict[str, str]
