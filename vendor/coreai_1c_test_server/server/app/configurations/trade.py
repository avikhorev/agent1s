"""Entity definitions for Trade Management configuration."""
from app.models.base import (
    _f, catalog_fields, document_fields, tabular_fields,
    accumulation_register_fields, information_register_fields,
)

_goods_tab = [
    _f("Номенклатура_Key","Edm.Guid"),
    _f("Количество","Edm.Decimal"),
    _f("Цена","Edm.Decimal"),
    _f("Сумма","Edm.Decimal"),
    _f("СтавкаНДС","Edm.String",max_length=20),
    _f("СуммаНДС","Edm.Decimal"),
]
_doc_cm = [
    _f("Контрагент_Key","Edm.Guid"),
    _f("Организация_Key","Edm.Guid"),
    _f("Склад_Key","Edm.Guid"),
    _f("Сумма","Edm.Decimal"),
    _f("Валюта","Edm.String",max_length=10),
    _f("Комментарий","Edm.String",max_length=500),
]
_reg_cm = [
    _f("Номенклатура_Key","Edm.Guid"),
    _f("Контрагент_Key","Edm.Guid"),
    _f("Склад_Key","Edm.Guid"),
    _f("Организация_Key","Edm.Guid"),
]

TRADE_CONFIG = {
    "name": "ut",
    "display_name": "Управление торговлей 11",
    "entities": {
        "Catalog_Номенклатура": {"table_name":"ut_catalog_nomenklatura","entity_type":"catalog","fields":catalog_fields(hierarchical=True,extra=[_f("Артикул","Edm.String",max_length=100),_f("ЕдиницаИзмерения","Edm.String",max_length=50),_f("ВидНоменклатуры","Edm.String",max_length=50),_f("Производитель","Edm.String",max_length=200),_f("ВесЕдиницы","Edm.Double",nullable=True)])},
        "Catalog_Контрагенты": {"table_name":"ut_catalog_kontragenty","entity_type":"catalog","fields":catalog_fields(hierarchical=True,extra=[_f("ИНН","Edm.String",max_length=12),_f("КПП","Edm.String",max_length=9),_f("ПолноеНаименование","Edm.String",max_length=500),_f("Телефон","Edm.String",max_length=50),_f("ЮридическийАдрес","Edm.String",max_length=500),_f("ЭтоПоставщик","Edm.Boolean"),_f("ЭтоКлиент","Edm.Boolean")])},
        "Catalog_Склады": {"table_name":"ut_catalog_sklady","entity_type":"catalog","fields":catalog_fields(extra=[_f("Город","Edm.String",max_length=100),_f("Адрес","Edm.String",max_length=500),_f("ТипСклада","Edm.String",max_length=50)])},
        "Catalog_Организации": {"table_name":"ut_catalog_organizatsii","entity_type":"catalog","fields":catalog_fields(extra=[_f("ИНН","Edm.String",max_length=12),_f("КПП","Edm.String",max_length=9),_f("ПолноеНаименование","Edm.String",max_length=500),_f("ЮридическийАдрес","Edm.String",max_length=500)])},
        "Catalog_ВидыЦен": {"table_name":"ut_catalog_vidy_tsen","entity_type":"catalog","fields":catalog_fields()},
        "Document_ЗаказКлиента": {"table_name":"ut_doc_zakaz_klienta","entity_type":"document","fields":document_fields(extra=_doc_cm+[_f("Статус","Edm.String",max_length=50)])},
        "Document_ЗаказКлиента_Товары": {"table_name":"ut_doc_zakaz_klienta_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=_goods_tab)},
        "Document_РеализацияТоваровУслуг": {"table_name":"ut_doc_realizatsiya","entity_type":"document","fields":document_fields(extra=_doc_cm)},
        "Document_РеализацияТоваровУслуг_Товары": {"table_name":"ut_doc_realizatsiya_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=_goods_tab)},
        "Document_ПоступлениеТоваровУслуг": {"table_name":"ut_doc_postupleniye","entity_type":"document","fields":document_fields(extra=_doc_cm)},
        "Document_ПоступлениеТоваровУслуг_Товары": {"table_name":"ut_doc_postupleniye_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=_goods_tab)},
        "Document_ВозвратТоваровОтКлиента": {"table_name":"ut_doc_vozvrat","entity_type":"document","fields":document_fields(extra=_doc_cm+[_f("ДокументОснование_Key","Edm.Guid",nullable=True)])},
        "Document_ВозвратТоваровОтКлиента_Товары": {"table_name":"ut_doc_vozvrat_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=_goods_tab)},
        "Document_УстановкаЦен": {"table_name":"ut_doc_ustanovka_tsen","entity_type":"document","fields":document_fields(extra=[_f("ВидЦен_Key","Edm.Guid"),_f("Комментарий","Edm.String",max_length=500)])},
        "Document_УстановкаЦен_Товары": {"table_name":"ut_doc_ustanovka_tsen_tovary","entity_type":"document_tabular","fields":tabular_fields(extra=[_f("Номенклатура_Key","Edm.Guid"),_f("Цена","Edm.Decimal")])},
        "AccumulationRegister_Продажи": {"table_name":"ut_reg_prodazhi","entity_type":"accumulation_register","fields":accumulation_register_fields(extra=_reg_cm+[_f("Количество","Edm.Decimal"),_f("Сумма","Edm.Decimal"),_f("Стоимость","Edm.Decimal")])},
        "AccumulationRegister_Закупки": {"table_name":"ut_reg_zakupki","entity_type":"accumulation_register","fields":accumulation_register_fields(extra=_reg_cm+[_f("Количество","Edm.Decimal"),_f("Сумма","Edm.Decimal")])},
        "AccumulationRegister_ТоварыНаСкладах": {"table_name":"ut_reg_tovary_na_skladakh","entity_type":"accumulation_register","fields":accumulation_register_fields(extra=[_f("Номенклатура_Key","Edm.Guid"),_f("Склад_Key","Edm.Guid"),_f("Количество","Edm.Decimal"),_f("ВидДвижения","Edm.String",max_length=20)])},
        "InformationRegister_ЦеныНоменклатуры": {"table_name":"ut_reg_tseny","entity_type":"information_register","fields":information_register_fields(extra=[_f("Номенклатура_Key","Edm.Guid"),_f("ВидЦен_Key","Edm.Guid"),_f("Цена","Edm.Decimal")])},
    },
}
