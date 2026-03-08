import pytest
from odata.metadata import parse_entity_fields

SAMPLE_EDMX = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
  <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
    <Schema xmlns="http://schemas.microsoft.com/ado/2008/09/edm" Namespace="StandardODATA">
      <EntityType Name="Catalog_Номенклатура">
        <Key><PropertyRef Name="Ref_Key"/></Key>
        <Property Name="Ref_Key" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Description" Type="Edm.String"/>
        <Property Name="Артикул" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""


def test_parse_fields():
    desc = parse_entity_fields(SAMPLE_EDMX, "Catalog_Номенклатура")
    assert desc.entity == "Catalog_Номенклатура"
    assert desc.fields == {
        "Ref_Key": "Edm.Guid",
        "Description": "Edm.String",
        "Артикул": "Edm.String",
    }


def test_unknown_entity_raises():
    with pytest.raises(KeyError, match="Unknown entity"):
        parse_entity_fields(SAMPLE_EDMX, "Nonexistent")
