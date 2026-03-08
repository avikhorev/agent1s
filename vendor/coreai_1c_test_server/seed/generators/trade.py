"""Generate realistic trade management data with analytical patterns."""
import json
import math
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4
from decimal import Decimal

from sqlalchemy import Table, MetaData, Column, String, Boolean, DateTime
from sqlalchemy import Integer, Float, Numeric, PrimaryKeyConstraint, insert
from sqlalchemy.dialects.postgresql import UUID


SEASONAL = {1:0.6,2:0.7,3:0.85,4:0.9,5:0.95,6:1.0,7:0.95,8:1.0,9:1.05,10:1.1,11:1.4,12:1.6}

DICT_DIR = os.path.join(os.path.dirname(__file__), "..", "dictionaries")


def _load(name):
    with open(os.path.join(DICT_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def _edm_col(name, edm, **kw):
    m = {"Edm.Guid": UUID(as_uuid=True), "Edm.String": String(kw.get("max_length", 255)),
         "Edm.Boolean": Boolean(), "Edm.DateTime": DateTime(),
         "Edm.Int32": Integer(), "Edm.Decimal": Numeric(15, 2), "Edm.Double": Float()}
    return Column(name, m.get(edm, String(255)), primary_key=kw.get("primary_key", False),
                  nullable=kw.get("nullable", not kw.get("primary_key", False)))


def _cat_cols(hierarchical=False, extra=None):
    cols = [_edm_col("Ref_Key","Edm.Guid",primary_key=True), _edm_col("DeletionMark","Edm.Boolean"),
            _edm_col("Description","Edm.String",max_length=200), _edm_col("Code","Edm.String",max_length=20)]
    if hierarchical:
        cols += [_edm_col("IsFolder","Edm.Boolean"), _edm_col("Parent_Key","Edm.Guid",nullable=True)]
    if extra:
        cols.extend(extra)
    return cols


def _doc_cols(extra=None):
    cols = [_edm_col("Ref_Key","Edm.Guid",primary_key=True), _edm_col("DeletionMark","Edm.Boolean"),
            _edm_col("Number","Edm.String",max_length=20), _edm_col("Date","Edm.DateTime"),
            _edm_col("Posted","Edm.Boolean")]
    if extra:
        cols.extend(extra)
    return cols


def _tab_cols(extra=None):
    cols = [_edm_col("Ref_Key","Edm.Guid"), _edm_col("LineNumber","Edm.Int32")]
    if extra:
        cols.extend(extra)
    return cols


def _reg_cols(extra=None):
    cols = [_edm_col("Ref_Key","Edm.Guid",primary_key=True), _edm_col("Period","Edm.DateTime"),
            _edm_col("Recorder_Key","Edm.Guid"), _edm_col("LineNumber","Edm.Int32"),
            _edm_col("Active","Edm.Boolean")]
    if extra:
        cols.extend(extra)
    return cols


def _ireg_cols(extra=None):
    cols = [_edm_col("Ref_Key","Edm.Guid",primary_key=True), _edm_col("Period","Edm.DateTime")]
    if extra:
        cols.extend(extra)
    return cols


def _goods_extra():
    return [_edm_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),
        _edm_col("\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e","Edm.Decimal"),_edm_col("\u0426\u0435\u043d\u0430","Edm.Decimal"),
        _edm_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_edm_col("\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421","Edm.String",max_length=20),
        _edm_col("\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421","Edm.Decimal")]

def _doc_extra():
    return [_edm_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),
        _edm_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid"),
        _edm_col("\u0421\u043a\u043b\u0430\u0434_Key","Edm.Guid"),_edm_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),
        _edm_col("\u0412\u0430\u043b\u044e\u0442\u0430","Edm.String",max_length=10),
        _edm_col("\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439","Edm.String",max_length=500)]


def create_ut_tables(meta):
    tables = {}
    tables["nomen"] = Table("ut_catalog_nomenklatura", meta, *_cat_cols(True, [
        _edm_col("\u0410\u0440\u0442\u0438\u043a\u0443\u043b","Edm.String",max_length=100),
        _edm_col("\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f","Edm.String",max_length=50),
        _edm_col("\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b","Edm.String",max_length=50),
        _edm_col("\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c","Edm.String",max_length=200),
        _edm_col("\u0412\u0435\u0441\u0415\u0434\u0438\u043d\u0438\u0446\u044b","Edm.Double",nullable=True)]))
    tables["kontr"] = Table("ut_catalog_kontragenty", meta, *_cat_cols(True, [
        _edm_col("\u0418\u041d\u041d","Edm.String",max_length=12),_edm_col("\u041a\u041f\u041f","Edm.String",max_length=9),
        _edm_col("\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435","Edm.String",max_length=500),
        _edm_col("\u0422\u0435\u043b\u0435\u0444\u043e\u043d","Edm.String",max_length=50),
        _edm_col("\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441","Edm.String",max_length=500),
        _edm_col("\u042d\u0442\u043e\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a","Edm.Boolean"),
        _edm_col("\u042d\u0442\u043e\u041a\u043b\u0438\u0435\u043d\u0442","Edm.Boolean")]))
    tables["sklady"] = Table("ut_catalog_sklady", meta, *_cat_cols(False, [
        _edm_col("\u0413\u043e\u0440\u043e\u0434","Edm.String",max_length=100),
        _edm_col("\u0410\u0434\u0440\u0435\u0441","Edm.String",max_length=500),
        _edm_col("\u0422\u0438\u043f\u0421\u043a\u043b\u0430\u0434\u0430","Edm.String",max_length=50)]))
    tables["org"] = Table("ut_catalog_organizatsii", meta, *_cat_cols(False, [
        _edm_col("\u0418\u041d\u041d","Edm.String",max_length=12),_edm_col("\u041a\u041f\u041f","Edm.String",max_length=9),
        _edm_col("\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435","Edm.String",max_length=500),
        _edm_col("\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441","Edm.String",max_length=500)]))
    tables["vidy_tsen"] = Table("ut_catalog_vidy_tsen", meta, *_cat_cols())
    tables["doc_zakaz"] = Table("ut_doc_zakaz_klienta", meta, *_doc_cols(_doc_extra()+[_edm_col("\u0421\u0442\u0430\u0442\u0443\u0441","Edm.String",max_length=50)]))
    tables["doc_zakaz_t"] = Table("ut_doc_zakaz_klienta_tovary", meta, *_tab_cols(_goods_extra()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    tables["doc_real"] = Table("ut_doc_realizatsiya", meta, *_doc_cols(_doc_extra()))
    tables["doc_real_t"] = Table("ut_doc_realizatsiya_tovary", meta, *_tab_cols(_goods_extra()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    tables["doc_post"] = Table("ut_doc_postupleniye", meta, *_doc_cols(_doc_extra()))
    tables["doc_post_t"] = Table("ut_doc_postupleniye_tovary", meta, *_tab_cols(_goods_extra()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    tables["doc_vozv"] = Table("ut_doc_vozvrat", meta, *_doc_cols(_doc_extra()+[_edm_col("\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442\u041e\u0441\u043d\u043e\u0432\u0430\u043d\u0438\u0435_Key","Edm.Guid",nullable=True)]))
    tables["doc_vozv_t"] = Table("ut_doc_vozvrat_tovary", meta, *_tab_cols(_goods_extra()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    tables["doc_ust"] = Table("ut_doc_ustanovka_tsen", meta, *_doc_cols([_edm_col("\u0412\u0438\u0434\u0426\u0435\u043d_Key","Edm.Guid"),_edm_col("\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439","Edm.String",max_length=500)]))
    tables["doc_ust_t"] = Table("ut_doc_ustanovka_tsen_tovary", meta, *_tab_cols([_edm_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),_edm_col("\u0426\u0435\u043d\u0430","Edm.Decimal")]), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    def _reg_cm():
        return [_edm_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),_edm_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),_edm_col("\u0421\u043a\u043b\u0430\u0434_Key","Edm.Guid"),_edm_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid")]
    tables["reg_prod"] = Table("ut_reg_prodazhi", meta, *_reg_cols(_reg_cm()+[_edm_col("\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e","Edm.Decimal"),_edm_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_edm_col("\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c","Edm.Decimal")]))
    tables["reg_zak"] = Table("ut_reg_zakupki", meta, *_reg_cols(_reg_cm()+[_edm_col("\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e","Edm.Decimal"),_edm_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal")]))
    tables["reg_sklad"] = Table("ut_reg_tovary_na_skladakh", meta, *_reg_cols([_edm_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),_edm_col("\u0421\u043a\u043b\u0430\u0434_Key","Edm.Guid"),_edm_col("\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e","Edm.Decimal"),_edm_col("\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f","Edm.String",max_length=20)]))
    tables["reg_tseny"] = Table("ut_reg_tseny", meta, *_ireg_cols([_edm_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),_edm_col("\u0412\u0438\u0434\u0426\u0435\u043d_Key","Edm.Guid"),_edm_col("\u0426\u0435\u043d\u0430","Edm.Decimal")]))
    return tables


def _bulk(conn, table, rows):
    if rows:
        conn.execute(insert(table), rows)


def seed_trade(conn, meta):
    print("  Creating UT tables...")
    T = create_ut_tables(meta)
    meta.create_all(conn.engine)

    products_data = _load("products.json")
    companies_data = _load("companies.json")
    cities_data = _load("cities.json")

    print("  Seeding catalogs...")
    # Organizations
    orgs = []
    for i, (name, inn, kpp) in enumerate([
        ("\u041e\u041e\u041e \u00ab\u0422\u043e\u0440\u0433\u041c\u0430\u0441\u0442\u0435\u0440\u00bb","7701111111","770101001"),
        ("\u0410\u041e \u00ab\u041c\u0435\u0433\u0430\u0422\u0440\u0435\u0439\u0434\u00bb","7802222222","780201001"),
    ]):
        orgs.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":name,"Code":f"ORG-{i+1:03d}",
                      "\u0418\u041d\u041d":inn,"\u041a\u041f\u041f":kpp,"\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":name,
                      "\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":f"\u0433. \u041c\u043e\u0441\u043a\u0432\u0430, \u0443\u043b. \u041f\u0440\u0438\u043c\u0435\u0440\u043d\u0430\u044f, {i+1}"})
    _bulk(conn, T["org"], orgs)

    # Price types
    vidy_tsen = []
    for i, name in enumerate(["\u0420\u043e\u0437\u043d\u0438\u0447\u043d\u0430\u044f","\u041e\u043f\u0442\u043e\u0432\u0430\u044f","\u0417\u0430\u043a\u0443\u043f\u043e\u0447\u043d\u0430\u044f"]):
        vidy_tsen.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":name,"Code":f"VT-{i+1:03d}"})
    _bulk(conn, T["vidy_tsen"], vidy_tsen)
    retail_key = vidy_tsen[0]["Ref_Key"]
    wholesale_key = vidy_tsen[1]["Ref_Key"]
    purchase_key = vidy_tsen[2]["Ref_Key"]

    # Warehouses
    warehouses = []
    wh_types = ["\u041c\u0430\u0433\u0430\u0437\u0438\u043d","\u041c\u0430\u0433\u0430\u0437\u0438\u043d","\u041c\u0430\u0433\u0430\u0437\u0438\u043d","\u041c\u0430\u0433\u0430\u0437\u0438\u043d","\u041c\u0430\u0433\u0430\u0437\u0438\u043d","\u0421\u043a\u043b\u0430\u0434","\u0421\u043a\u043b\u0430\u0434","\u0421\u043a\u043b\u0430\u0434"]
    for i, (city, wtype) in enumerate(zip(cities_data[:8], wh_types)):
        addr = city["addresses"][0] if city["addresses"] else ""
        nm = f"{wtype} \u00ab{city['name']}\u00bb"
        warehouses.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":nm,"Code":f"WH-{i+1:03d}",
                           "\u0413\u043e\u0440\u043e\u0434":city["name"],"\u0410\u0434\u0440\u0435\u0441":addr,"\u0422\u0438\u043f\u0421\u043a\u043b\u0430\u0434\u0430":wtype})
    _bulk(conn, T["sklady"], warehouses)
    worst_wh = warehouses[3]

    # Products
    all_products = []
    group_keys = {}
    code_n = 1
    for grp in products_data["groups"]:
        gkey = uuid4()
        group_keys[grp["name"]] = gkey
        all_products.append({"Ref_Key":gkey,"DeletionMark":False,"Description":grp["name"],
            "Code":f"{code_n:06d}","IsFolder":True,"Parent_Key":None,
            "\u0410\u0440\u0442\u0438\u043a\u0443\u043b":"","\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f":"",
            "\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b":"\u0413\u0440\u0443\u043f\u043f\u0430",
            "\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c":"","\u0412\u0435\u0441\u0415\u0434\u0438\u043d\u0438\u0446\u044b":None})
        code_n += 1
        for item in grp["items"]:
            all_products.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":item["name"],
                "Code":f"{code_n:06d}","IsFolder":False,"Parent_Key":gkey,
                "\u0410\u0440\u0442\u0438\u043a\u0443\u043b":item["article"],
                "\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f":item["unit"],
                "\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b":"\u0422\u043e\u0432\u0430\u0440",
                "\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c":item["manufacturer"],
                "\u0412\u0435\u0441\u0415\u0434\u0438\u043d\u0438\u0446\u044b":item.get("weight")})
            code_n += 1
    _bulk(conn, T["nomen"], all_products)
    items_only = [p for p in all_products if not p["IsFolder"]]

    # Counterparties
    counterparties = []
    code_n = 1
    sup_group = uuid4()
    cli_group = uuid4()
    counterparties.append({"Ref_Key":sup_group,"DeletionMark":False,"Description":"\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a\u0438",
        "Code":f"K-{code_n:03d}","IsFolder":True,"Parent_Key":None,
        "\u0418\u041d\u041d":"","\u041a\u041f\u041f":"","\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":"\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a\u0438",
        "\u0422\u0435\u043b\u0435\u0444\u043e\u043d":"","\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":"",
        "\u042d\u0442\u043e\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a":True,"\u042d\u0442\u043e\u041a\u043b\u0438\u0435\u043d\u0442":False})
    code_n += 1
    counterparties.append({"Ref_Key":cli_group,"DeletionMark":False,"Description":"\u041a\u043b\u0438\u0435\u043d\u0442\u044b",
        "Code":f"K-{code_n:03d}","IsFolder":True,"Parent_Key":None,
        "\u0418\u041d\u041d":"","\u041a\u041f\u041f":"","\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":"\u041a\u043b\u0438\u0435\u043d\u0442\u044b",
        "\u0422\u0435\u043b\u0435\u0444\u043e\u043d":"","\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":"",
        "\u042d\u0442\u043e\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a":False,"\u042d\u0442\u043e\u041a\u043b\u0438\u0435\u043d\u0442":True})
    code_n += 1

    suppliers = []
    for s in companies_data["suppliers"]:
        k = uuid4()
        counterparties.append({"Ref_Key":k,"DeletionMark":False,"Description":s["name"],
            "Code":f"K-{code_n:03d}","IsFolder":False,"Parent_Key":sup_group,
            "\u0418\u041d\u041d":s["inn"],"\u041a\u041f\u041f":s.get("kpp",""),
            "\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":s["name"],
            "\u0422\u0435\u043b\u0435\u0444\u043e\u043d":f"+7-{random.randint(900,999)}-{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(10,99)}",
            "\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":f"\u0433. {s['city']}",
            "\u042d\u0442\u043e\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a":True,"\u042d\u0442\u043e\u041a\u043b\u0438\u0435\u043d\u0442":False})
        suppliers.append(k)
        code_n += 1

    clients = []
    for c in companies_data["clients"]:
        k = uuid4()
        counterparties.append({"Ref_Key":k,"DeletionMark":False,"Description":c["name"],
            "Code":f"K-{code_n:03d}","IsFolder":False,"Parent_Key":cli_group,
            "\u0418\u041d\u041d":c["inn"],"\u041a\u041f\u041f":c.get("kpp",""),
            "\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":c["name"],
            "\u0422\u0435\u043b\u0435\u0444\u043e\u043d":f"+7-{random.randint(900,999)}-{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(10,99)}",
            "\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":f"\u0433. {c['city']}",
            "\u042d\u0442\u043e\u041f\u043e\u0441\u0442\u0430\u0432\u0449\u0438\u043a":False,"\u042d\u0442\u043e\u041a\u043b\u0438\u0435\u043d\u0442":True})
        clients.append(k)
        code_n += 1
    _bulk(conn, T["kontr"], counterparties)
    problem_supplier = suppliers[-1]

    # Prices for products
    prices = {}
    for p in items_only:
        base = random.uniform(50, 80000)
        prices[p["Ref_Key"]] = {"purchase": round(base * 0.6, 2), "wholesale": round(base * 0.85, 2), "retail": round(base, 2)}

    # High return products
    high_return_products = set(p["Ref_Key"] for p in items_only[:8])
    # Low margin products
    for p in items_only[40:45]:
        pr = prices[p["Ref_Key"]]
        pr["purchase"] = round(pr["retail"] * 0.97, 2)

    print("  Generating documents (2 years)...")
    doc_num = 1
    org_key = orgs[0]["Ref_Key"]
    start = datetime(2024, 1, 1)
    end = datetime(2025, 12, 31)
    day = start

    all_sales = []
    all_sales_t = []
    all_purchases = []
    all_purchases_t = []
    all_orders = []
    all_orders_t = []
    all_returns = []
    all_returns_t = []
    reg_prodazhi = []
    reg_zakupki = []
    reg_sklad = []

    while day <= end:
        month = day.month
        sf = SEASONAL[month]
        days_elapsed = (day - start).days
        decline_factor = max(0.4, 1.0 - days_elapsed * 0.0008)

        # Purchases (~1-2 per day)
        if random.random() < 0.4:
            supplier = random.choice(suppliers)
            wh = random.choice(warehouses)
            dk = uuid4()
            items_count = random.randint(3, 10)
            total = Decimal(0)
            rows = []
            for ln in range(1, items_count + 1):
                prod = random.choice(items_only)
                qty = Decimal(random.randint(10, 200))
                price = Decimal(str(prices[prod["Ref_Key"]]["purchase"]))
                s = qty * price
                nds = round(s * Decimal("0.2"), 2)
                total += s
                rows.append({"Ref_Key":dk,"LineNumber":ln,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0426\u0435\u043d\u0430":price,
                    "\u0421\u0443\u043c\u043c\u0430":s,"\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421":"20%","\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds})
                reg_zakupki.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":ln,"Active":True,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],"\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":supplier,
                    "\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0421\u0443\u043c\u043c\u0430":s})
                reg_sklad.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":ln,"Active":True,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],"\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f":"\u041f\u0440\u0438\u0445\u043e\u0434"})
            all_purchases.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"P-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":supplier,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],"\u0421\u0443\u043c\u043c\u0430":total,"\u0412\u0430\u043b\u044e\u0442\u0430":"RUB","\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439":""})
            all_purchases_t.extend(rows)
            doc_num += 1

        # Sales (~8-20 per day depending on season)
        num_sales = max(1, int(random.gauss(12, 3) * sf))
        for _ in range(num_sales):
            client = random.choice(clients)
            wh = random.choice(warehouses)
            if wh["Ref_Key"] == worst_wh["Ref_Key"] and random.random() > decline_factor:
                continue
            dk = uuid4()
            items_count = random.randint(1, 6)
            total = Decimal(0)
            rows = []
            for ln in range(1, items_count + 1):
                prod = random.choice(items_only)
                qty = Decimal(random.randint(1, 20))
                price = Decimal(str(prices[prod["Ref_Key"]]["retail"]))
                s = qty * price
                nds = round(s * Decimal("0.2"), 2)
                total += s
                rows.append({"Ref_Key":dk,"LineNumber":ln,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0426\u0435\u043d\u0430":price,
                    "\u0421\u0443\u043c\u043c\u0430":s,"\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421":"20%","\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds})
                cost = Decimal(str(prices[prod["Ref_Key"]]["purchase"])) * qty
                reg_prodazhi.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":ln,"Active":True,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],"\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":client,
                    "\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0421\u0443\u043c\u043c\u0430":s,"\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c":cost})
                reg_sklad.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":ln,"Active":True,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],"\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f":"\u0420\u0430\u0441\u0445\u043e\u0434"})
            all_sales.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"S-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":client,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],"\u0421\u0443\u043c\u043c\u0430":total,"\u0412\u0430\u043b\u044e\u0442\u0430":"RUB","\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439":""})
            all_sales_t.extend(rows)
            doc_num += 1

        # Returns (~5% of sales, higher for high_return products)
        if random.random() < 0.08:
            client = random.choice(clients)
            wh = random.choice(warehouses)
            dk = uuid4()
            prod = random.choice(items_only)
            is_high = prod["Ref_Key"] in high_return_products
            if not is_high and random.random() > 0.3:
                pass
            else:
                qty = Decimal(random.randint(1, 5))
                price = Decimal(str(prices[prod["Ref_Key"]]["retail"]))
                s = qty * price
                nds = round(s * Decimal("0.2"), 2)
                all_returns.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"R-{doc_num:06d}","Date":day,"Posted":True,
                    "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":client,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                    "\u0421\u043a\u043b\u0430\u0434_Key":wh["Ref_Key"],"\u0421\u0443\u043c\u043c\u0430":s,"\u0412\u0430\u043b\u044e\u0442\u0430":"RUB",
                    "\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439":"","\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442\u041e\u0441\u043d\u043e\u0432\u0430\u043d\u0438\u0435_Key":None})
                all_returns_t.append({"Ref_Key":dk,"LineNumber":1,
                    "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0426\u0435\u043d\u0430":price,
                    "\u0421\u0443\u043c\u043c\u0430":s,"\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421":"20%","\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds})
                doc_num += 1

        day += timedelta(days=1)

    print(f"  Inserting {len(all_sales)} sales, {len(all_purchases)} purchases, {len(all_returns)} returns...")
    _bulk(conn, T["doc_real"], all_sales)
    _bulk(conn, T["doc_real_t"], all_sales_t)
    _bulk(conn, T["doc_post"], all_purchases)
    _bulk(conn, T["doc_post_t"], all_purchases_t)
    _bulk(conn, T["doc_vozv"], all_returns)
    _bulk(conn, T["doc_vozv_t"], all_returns_t)

    print(f"  Inserting {len(reg_prodazhi)} sales register records...")
    _bulk(conn, T["reg_prod"], reg_prodazhi)
    _bulk(conn, T["reg_zak"], reg_zakupki)
    _bulk(conn, T["reg_sklad"], reg_sklad)

    # Price register
    price_regs = []
    for p in items_only:
        pr = prices[p["Ref_Key"]]
        for vt_key, price_type in [(retail_key, "retail"), (wholesale_key, "wholesale"), (purchase_key, "purchase")]:
            price_regs.append({"Ref_Key":uuid4(),"Period":datetime(2024,1,1),
                "\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":p["Ref_Key"],
                "\u0412\u0438\u0434\u0426\u0435\u043d_Key":vt_key,"\u0426\u0435\u043d\u0430":Decimal(str(pr[price_type]))})
    _bulk(conn, T["reg_tseny"], price_regs)

    conn.commit()
    print(f"  UT seeding complete: {len(all_products)} products, {len(all_sales)} sales, {len(all_purchases)} purchases, {len(all_returns)} returns")
