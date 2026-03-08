from xml.etree import ElementTree

from odata.types import EntityDescription

EDM_NS = {"edm": "http://schemas.microsoft.com/ado/2008/09/edm"}


def parse_entity_fields(metadata_xml: str, entity_name: str) -> EntityDescription:
    root = ElementTree.fromstring(metadata_xml)
    entity_xpath = f".//edm:EntityType[@Name='{entity_name}']"
    entity_node = root.find(entity_xpath, EDM_NS)
    if entity_node is None:
        raise KeyError(f"Unknown entity: {entity_name}")

    fields: dict[str, str] = {}
    for prop in entity_node.findall("edm:Property", EDM_NS):
        field_name = prop.attrib["Name"]
        fields[field_name] = prop.attrib["Type"]
    return EntityDescription(entity=entity_name, fields=fields)
