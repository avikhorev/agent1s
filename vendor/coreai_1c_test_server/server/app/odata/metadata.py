"""Generate EDMX $metadata XML for 1C OData configurations."""
from xml.etree.ElementTree import Element, SubElement, tostring

EDM_MAP = {
    "Edm.Guid":"Edm.Guid","Edm.String":"Edm.String","Edm.Boolean":"Edm.Boolean",
    "Edm.DateTime":"Edm.DateTime","Edm.Int32":"Edm.Int32","Edm.Int64":"Edm.Int64",
    "Edm.Decimal":"Edm.Decimal","Edm.Double":"Edm.Double",
}


def generate_metadata_xml(config, base_url):
    ns_edmx = "http://schemas.microsoft.com/ado/2007/06/edmx"
    ns_m = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
    ns_edm = "http://schemas.microsoft.com/ado/2008/09/edm"

    edmx = Element("edmx:Edmx", {"xmlns:edmx": ns_edmx, "Version": "1.0"})
    ds = SubElement(edmx, "edmx:DataServices", {"m:DataServiceVersion": "3.0", "xmlns:m": ns_m})
    schema = SubElement(ds, "Schema", {"xmlns": ns_edm, "Namespace": "StandardODATA"})
    container = SubElement(schema, "EntityContainer", {"Name": "StandardODATA", "m:IsDefaultEntityContainer": "true"})

    for ename, edef in config["entities"].items():
        et = SubElement(schema, "EntityType", {"Name": ename})
        key_el = SubElement(et, "Key")
        for field in edef["fields"]:
            if field.get("primary_key"):
                SubElement(key_el, "PropertyRef", {"Name": field["name"]})
            attrs = {"Name": field["name"], "Type": EDM_MAP.get(field["edm_type"], "Edm.String"), "Nullable": str(field.get("nullable", True)).lower()}
            if field["edm_type"] == "Edm.String":
                attrs["MaxLength"] = str(field.get("max_length", 255))
            SubElement(et, "Property", attrs)
        SubElement(container, "EntitySet", {"Name": ename, "EntityType": f"StandardODATA.{ename}"})

    xml_str = tostring(edmx, encoding="unicode", xml_declaration=False)
    return '<?xml version="1.0" encoding="utf-8"?>\n' + xml_str
