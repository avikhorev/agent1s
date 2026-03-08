"""Entity definitions for the Accounting configuration."""
from app.models.base import (
    _f, catalog_fields, document_fields, tabular_fields,
    accumulation_register_fields,
)

ACCOUNTING_CONFIG = {
    "name": "bp",
    "display_name": "Бухгалтерия предприятия 3.0",
    "entities": {
        "Catalog_Номенклатура": {"table_name":"bp_catalog_nomenklatura","entity_type":"catalog","fields":catalog_fields(hierarchical=True,extra=[_f("ЕдиницаИзмерения","Edm.String",max_length=50),_f("ВидНоменклатуры","Edm.String",max_length=50)])},
        "Catalog_Контрагенты": {"table_name":"bp_catalog_kontragenty","entity_type":"catalog","fields":catalog_fields(hierarchical=True,extra=[_f("ИНН","Edm.String",max_length=12),_f("КПП","Edm.String",max_length=9),_f("ПолноеНаименование","Edm.String",max_length=500),_f("Телефон","Edm.String",max_length=50),_f("ЮридическийАдрес","Edm.String",max_length=500)])},
        "Catalog_Организации": {"table_name":"bp_catalog_organizatsii","entity_type":"catalog","fields":catalog_fields(extra=[_f("ИНН","Edm.String",max_length=12),_f("КПП","Edm.String",max_length=9),_f("ПолноеНаименование","Edm.String",max_length=500),_f("ЮридическийАдрес","Edm.String",max_length=500)])},
        "Catalog_СтатьиЗатрат": {"table_name":"bp_catalog_statji_zatrat","entity_type":"catalog","fields":catalog_fields(extra=[_f("ВидРасходов","Edm.String",max_length=100)])},
        "Catalog_Подразделения": {"table_name":"bp_catalog_podrazdeleniya","entity_type":"catalog","fields":catalog_fields(extra=[_f("Руководитель","Edm.String",max_length=200)])},
        "Document_РеализацияТоваровУслуг": {"table_name":"bp_doc_realizatsiya","entity_type":"document","fields":document_fields(extra=[_f("Контрагент_Key","Edm.Guid"),_f("Организация_Key","Edm.Guid"),_f("Подразделение_Key","Edm.Guid",nullable=True),_f("Сумма","Edm.Decimal"),_f("СуммаНДС","Edm.Decimal"),_f("Валюта","Edm.String",max_length=10)])},
        "Document_РеализацияТоваровУслуг_Товары": {"table_name":"bp_doc_realizatsiya_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=[_f("Номенклатура_Key","Edm.Guid"),_f("Количество","Edm.Decimal"),_f("Цена","Edm.Decimal"),_f("Сумма","Edm.Decimal"),_f("СтавкаНДС","Edm.String",max_length=20),_f("СуммаНДС","Edm.Decimal"),_f("СчетУчета","Edm.String",max_length=20)])},
        "Document_ПоступлениеТоваровУслуг": {"table_name":"bp_doc_postupleniye","entity_type":"document","fields":document_fields(extra=[_f("Контрагент_Key","Edm.Guid"),_f("Организация_Key","Edm.Guid"),_f("Подразделение_Key","Edm.Guid",nullable=True),_f("Сумма","Edm.Decimal"),_f("СуммаНДС","Edm.Decimal"),_f("Валюта","Edm.String",max_length=10)])},
        "Document_ПоступлениеТоваровУслуг_Товары": {"table_name":"bp_doc_postupleniye_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=[_f("Номенклатура_Key","Edm.Guid"),_f("Количество","Edm.Decimal"),_f("Цена","Edm.Decimal"),_f("Сумма","Edm.Decimal"),_f("СтавкаНДС","Edm.String",max_length=20),_f("СуммаНДС","Edm.Decimal"),_f("СчетУчета","Edm.String",max_length=20)])},
        "Document_ПлатежноеПоручение": {"table_name":"bp_doc_platezhnoe","entity_type":"document","fields":document_fields(extra=[_f("Контрагент_Key","Edm.Guid"),_f("Организация_Key","Edm.Guid"),_f("Сумма","Edm.Decimal"),_f("НазначениеПлатежа","Edm.String",max_length=500),_f("ВидОплаты","Edm.String",max_length=50),_f("РасчетныйСчет","Edm.String",max_length=30)])},
        "Document_ПоступлениеНаРасчетныйСчет": {"table_name":"bp_doc_postupleniye_na_rs","entity_type":"document","fields":document_fields(extra=[_f("Контрагент_Key","Edm.Guid"),_f("Организация_Key","Edm.Guid"),_f("Сумма","Edm.Decimal"),_f("НазначениеПлатежа","Edm.String",max_length=500),_f("ВидПоступления","Edm.String",max_length=50),_f("РасчетныйСчет","Edm.String",max_length=30)])},
        "AccumulationRegister_ВзаиморасчетыСКонтрагентами": {"table_name":"bp_reg_vzaimoraschet","entity_type":"accumulation_register","fields":accumulation_register_fields(extra=[_f("Контрагент_Key","Edm.Guid"),_f("Организация_Key","Edm.Guid"),_f("Сумма","Edm.Decimal"),_f("ВидДвижения","Edm.String",max_length=20)])},
    },
}
