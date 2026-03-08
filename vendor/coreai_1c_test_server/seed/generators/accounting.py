"""Generate accounting data for BP configuration."""
import json
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4
from decimal import Decimal
from sqlalchemy import Table, MetaData, Column, String, Boolean, DateTime
from sqlalchemy import Integer, Float, Numeric, PrimaryKeyConstraint, insert
from sqlalchemy.dialects.postgresql import UUID


DICT_DIR = os.path.join(os.path.dirname(__file__), "..", "dictionaries")

def _load(name):
    with open(os.path.join(DICT_DIR, name), encoding="utf-8") as f:
        return json.load(f)

def _col(name, edm, **kw):
    m = {"Edm.Guid": UUID(as_uuid=True), "Edm.String": String(kw.get("max_length",255)),
         "Edm.Boolean": Boolean(), "Edm.DateTime": DateTime(),
         "Edm.Int32": Integer(), "Edm.Decimal": Numeric(15,2), "Edm.Double": Float()}
    return Column(name, m.get(edm, String(255)), primary_key=kw.get("primary_key",False),
                  nullable=kw.get("nullable", not kw.get("primary_key",False)))

def _cat(h=False, extra=None):
    cols = [_col("Ref_Key","Edm.Guid",primary_key=True),_col("DeletionMark","Edm.Boolean"),
            _col("Description","Edm.String",max_length=200),_col("Code","Edm.String",max_length=20)]
    if h:
        cols += [_col("IsFolder","Edm.Boolean"),_col("Parent_Key","Edm.Guid",nullable=True)]
    if extra: cols.extend(extra)
    return cols

def _doc(extra=None):
    cols = [_col("Ref_Key","Edm.Guid",primary_key=True),_col("DeletionMark","Edm.Boolean"),
            _col("Number","Edm.String",max_length=20),_col("Date","Edm.DateTime"),_col("Posted","Edm.Boolean")]
    if extra: cols.extend(extra)
    return cols

def _tab(extra=None):
    cols = [_col("Ref_Key","Edm.Guid"),_col("LineNumber","Edm.Int32")]
    if extra: cols.extend(extra)
    return cols

def _reg(extra=None):
    cols = [_col("Ref_Key","Edm.Guid",primary_key=True),_col("Period","Edm.DateTime"),
            _col("Recorder_Key","Edm.Guid"),_col("LineNumber","Edm.Int32"),_col("Active","Edm.Boolean")]
    if extra: cols.extend(extra)
    return cols

def _bulk(conn, table, rows):
    if rows: conn.execute(insert(table), rows)

def create_bp_tables(meta):
    T = {}
    T["nomen"] = Table("bp_catalog_nomenklatura", meta, *_cat(True,[_col("\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f","Edm.String",max_length=50),_col("\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b","Edm.String",max_length=50)]))
    T["kontr"] = Table("bp_catalog_kontragenty", meta, *_cat(True,[_col("\u0418\u041d\u041d","Edm.String",max_length=12),_col("\u041a\u041f\u041f","Edm.String",max_length=9),_col("\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435","Edm.String",max_length=500),_col("\u0422\u0435\u043b\u0435\u0444\u043e\u043d","Edm.String",max_length=50),_col("\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441","Edm.String",max_length=500)]))
    T["org"] = Table("bp_catalog_organizatsii", meta, *_cat(False,[_col("\u0418\u041d\u041d","Edm.String",max_length=12),_col("\u041a\u041f\u041f","Edm.String",max_length=9),_col("\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435","Edm.String",max_length=500),_col("\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441","Edm.String",max_length=500)]))
    T["zatrat"] = Table("bp_catalog_statji_zatrat", meta, *_cat(False,[_col("\u0412\u0438\u0434\u0420\u0430\u0441\u0445\u043e\u0434\u043e\u0432","Edm.String",max_length=100)]))
    T["podr"] = Table("bp_catalog_podrazdeleniya", meta, *_cat(False,[_col("\u0420\u0443\u043a\u043e\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c","Edm.String",max_length=200)]))
    def _bp_doc():
        return [_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid"),_col("\u041f\u043e\u0434\u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u0438\u0435_Key","Edm.Guid",nullable=True),_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_col("\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421","Edm.Decimal"),_col("\u0412\u0430\u043b\u044e\u0442\u0430","Edm.String",max_length=10)]
    def _bp_tab():
        return [_col("\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key","Edm.Guid"),_col("\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e","Edm.Decimal"),_col("\u0426\u0435\u043d\u0430","Edm.Decimal"),_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_col("\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421","Edm.String",max_length=20),_col("\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421","Edm.Decimal"),_col("\u0421\u0447\u0435\u0442\u0423\u0447\u0435\u0442\u0430","Edm.String",max_length=20)]
    T["doc_real"] = Table("bp_doc_realizatsiya", meta, *_doc(_bp_doc()))
    T["doc_real_t"] = Table("bp_doc_realizatsiya_tovary", meta, *_tab(_bp_tab()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    T["doc_post"] = Table("bp_doc_postupleniye", meta, *_doc(_bp_doc()))
    T["doc_post_t"] = Table("bp_doc_postupleniye_tovary", meta, *_tab(_bp_tab()), PrimaryKeyConstraint("Ref_Key","LineNumber"))
    T["doc_plat"] = Table("bp_doc_platezhnoe", meta, *_doc([_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid"),_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_col("\u041d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435\u041f\u043b\u0430\u0442\u0435\u0436\u0430","Edm.String",max_length=500),_col("\u0412\u0438\u0434\u041e\u043f\u043b\u0430\u0442\u044b","Edm.String",max_length=50),_col("\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439\u0421\u0447\u0435\u0442","Edm.String",max_length=30)]))
    T["doc_post_rs"] = Table("bp_doc_postupleniye_na_rs", meta, *_doc([_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid"),_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_col("\u041d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435\u041f\u043b\u0430\u0442\u0435\u0436\u0430","Edm.String",max_length=500),_col("\u0412\u0438\u0434\u041f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u044f","Edm.String",max_length=50),_col("\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439\u0421\u0447\u0435\u0442","Edm.String",max_length=30)]))
    T["reg_vz"] = Table("bp_reg_vzaimoraschet", meta, *_reg([_col("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key","Edm.Guid"),_col("\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key","Edm.Guid"),_col("\u0421\u0443\u043c\u043c\u0430","Edm.Decimal"),_col("\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f","Edm.String",max_length=20)]))
    return T


def seed_accounting(conn, meta):
    print("  Creating BP tables...")
    T = create_bp_tables(meta)
    meta.create_all(conn.engine)

    companies = _load("companies.json")
    products = _load("products.json")

    org_key = uuid4()
    _bulk(conn, T["org"], [{"Ref_Key":org_key,"DeletionMark":False,"Description":"\u041e\u041e\u041e \u00ab\u0422\u043e\u0440\u0433\u041c\u0430\u0441\u0442\u0435\u0440\u00bb","Code":"ORG-001",
        "\u0418\u041d\u041d":"7701111111","\u041a\u041f\u041f":"770101001","\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":"\u041e\u041e\u041e \u00ab\u0422\u043e\u0440\u0433\u041c\u0430\u0441\u0442\u0435\u0440\u00bb",
        "\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":"\u0433. \u041c\u043e\u0441\u043a\u0432\u0430"}])

    # Departments
    deps = []
    for i, (n, r) in enumerate([("\u041e\u0442\u0434\u0435\u043b \u043f\u0440\u043e\u0434\u0430\u0436","\u0418\u0432\u0430\u043d\u043e\u0432 \u0418.\u0418."),("\u041e\u0442\u0434\u0435\u043b \u0437\u0430\u043a\u0443\u043f\u043e\u043a","\u041f\u0435\u0442\u0440\u043e\u0432 \u041f.\u041f."),("\u0411\u0443\u0445\u0433\u0430\u043b\u0442\u0435\u0440\u0438\u044f","\u0421\u0438\u0434\u043e\u0440\u043e\u0432\u0430 \u0421.\u0421.")]):
        deps.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":n,"Code":f"D-{i+1:03d}","\u0420\u0443\u043a\u043e\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c":r})
    _bulk(conn, T["podr"], deps)

    # Cost items
    costs = []
    for i, (n, v) in enumerate([("\u041c\u0430\u0442\u0435\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0435 \u0440\u0430\u0441\u0445\u043e\u0434\u044b","\u041c\u0430\u0442\u0435\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0435"),("\u041e\u043f\u043b\u0430\u0442\u0430 \u0442\u0440\u0443\u0434\u0430","\u041e\u043f\u043b\u0430\u0442\u0430 \u0442\u0440\u0443\u0434\u0430"),("\u0410\u0440\u0435\u043d\u0434\u0430","\u041f\u0440\u043e\u0447\u0438\u0435"),("\u0422\u0440\u0430\u043d\u0441\u043f\u043e\u0440\u0442\u043d\u044b\u0435","\u041f\u0440\u043e\u0447\u0438\u0435")]):
        costs.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":n,"Code":f"Z-{i+1:03d}","\u0412\u0438\u0434\u0420\u0430\u0441\u0445\u043e\u0434\u043e\u0432":v})
    _bulk(conn, T["zatrat"], costs)

    # Products (simplified)
    bp_products = []
    code_n = 1
    for grp in products["groups"][:3]:
        gk = uuid4()
        bp_products.append({"Ref_Key":gk,"DeletionMark":False,"Description":grp["name"],"Code":f"{code_n:06d}","IsFolder":True,"Parent_Key":None,"\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f":"","\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b":"\u0413\u0440\u0443\u043f\u043f\u0430"})
        code_n += 1
        for item in grp["items"][:6]:
            bp_products.append({"Ref_Key":uuid4(),"DeletionMark":False,"Description":item["name"],"Code":f"{code_n:06d}","IsFolder":False,"Parent_Key":gk,"\u0415\u0434\u0438\u043d\u0438\u0446\u0430\u0418\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f":item["unit"],"\u0412\u0438\u0434\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u044b":"\u0422\u043e\u0432\u0430\u0440"})
            code_n += 1
    _bulk(conn, T["nomen"], bp_products)
    bp_items = [p for p in bp_products if not p.get("IsFolder")]

    # Counterparties
    bp_kontrs = []
    code_n = 1
    all_keys = []
    for c in companies["suppliers"] + companies["clients"]:
        k = uuid4()
        all_keys.append(k)
        bp_kontrs.append({"Ref_Key":k,"DeletionMark":False,"Description":c["name"],"Code":f"K-{code_n:03d}","IsFolder":False,"Parent_Key":None,
            "\u0418\u041d\u041d":c["inn"],"\u041a\u041f\u041f":c.get("kpp",""),"\u041f\u043e\u043b\u043d\u043e\u0435\u041d\u0430\u0438\u043c\u0435\u043d\u043e\u0432\u0430\u043d\u0438\u0435":c["name"],
            "\u0422\u0435\u043b\u0435\u0444\u043e\u043d":"","\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439\u0410\u0434\u0440\u0435\u0441":f"\u0433. {c['city']}"})
        code_n += 1
    _bulk(conn, T["kontr"], bp_kontrs)

    print("  Generating BP documents...")
    doc_num = 1
    start = datetime(2024, 1, 1)
    end = datetime(2025, 12, 31)
    day = start
    sales = []; sales_t = []; purchases = []; purchases_t = []
    payments = []; receipts = []; reg_vz = []

    while day <= end:
        # Sales (~3/day)
        for _ in range(random.randint(1, 5)):
            dk = uuid4()
            kontr = random.choice(all_keys)
            dep = random.choice(deps)["Ref_Key"]
            total = Decimal(0)
            rows = []
            for ln in range(1, random.randint(2, 5)):
                prod = random.choice(bp_items)
                qty = Decimal(random.randint(1, 30))
                price = Decimal(str(round(random.uniform(100, 50000), 2)))
                s = qty * price
                nds = round(s * Decimal("0.2"), 2)
                total += s
                rows.append({"Ref_Key":dk,"LineNumber":ln,"\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0426\u0435\u043d\u0430":price,"\u0421\u0443\u043c\u043c\u0430":s,
                    "\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421":"20%","\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds,"\u0421\u0447\u0435\u0442\u0423\u0447\u0435\u0442\u0430":"90.01"})
            nds_total = round(total * Decimal("0.2"), 2)
            sales.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"BP-S-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u041f\u043e\u0434\u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u0438\u0435_Key":dep,"\u0421\u0443\u043c\u043c\u0430":total,"\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds_total,"\u0412\u0430\u043b\u044e\u0442\u0430":"RUB"})
            sales_t.extend(rows)
            reg_vz.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":1,"Active":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u0443\u043c\u043c\u0430":total,"\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f":"\u041f\u0440\u0438\u0445\u043e\u0434"})
            doc_num += 1

        # Purchase (~1/day)
        if random.random() < 0.5:
            dk = uuid4()
            kontr = random.choice(all_keys[:len(companies["suppliers"])])
            total = Decimal(0)
            rows = []
            for ln in range(1, random.randint(2, 6)):
                prod = random.choice(bp_items)
                qty = Decimal(random.randint(5, 50))
                price = Decimal(str(round(random.uniform(80, 40000), 2)))
                s = qty * price; nds = round(s * Decimal("0.2"), 2); total += s
                rows.append({"Ref_Key":dk,"LineNumber":ln,"\u041d\u043e\u043c\u0435\u043d\u043a\u043b\u0430\u0442\u0443\u0440\u0430_Key":prod["Ref_Key"],
                    "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e":qty,"\u0426\u0435\u043d\u0430":price,"\u0421\u0443\u043c\u043c\u0430":s,
                    "\u0421\u0442\u0430\u0432\u043a\u0430\u041d\u0414\u0421":"20%","\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds,"\u0421\u0447\u0435\u0442\u0423\u0447\u0435\u0442\u0430":"41.01"})
            nds_total = round(total * Decimal("0.2"), 2)
            purchases.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"BP-P-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u041f\u043e\u0434\u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u0438\u0435_Key":None,"\u0421\u0443\u043c\u043c\u0430":total,"\u0421\u0443\u043c\u043c\u0430\u041d\u0414\u0421":nds_total,"\u0412\u0430\u043b\u044e\u0442\u0430":"RUB"})
            purchases_t.extend(rows)
            reg_vz.append({"Ref_Key":uuid4(),"Period":day,"Recorder_Key":dk,"LineNumber":1,"Active":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u0443\u043c\u043c\u0430":total,"\u0412\u0438\u0434\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u044f":"\u0420\u0430\u0441\u0445\u043e\u0434"})
            doc_num += 1

        # Payments
        if random.random() < 0.3:
            dk = uuid4()
            kontr = random.choice(all_keys)
            s = Decimal(str(round(random.uniform(10000, 500000), 2)))
            payments.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"BP-PP-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u0443\u043c\u043c\u0430":s,"\u041d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435\u041f\u043b\u0430\u0442\u0435\u0436\u0430":"\u041e\u043f\u043b\u0430\u0442\u0430 \u043f\u043e \u0434\u043e\u0433\u043e\u0432\u043e\u0440\u0443",
                "\u0412\u0438\u0434\u041e\u043f\u043b\u0430\u0442\u044b":"\u0411\u0435\u0437\u043d\u0430\u043b\u0438\u0447\u043d\u044b\u0435","\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439\u0421\u0447\u0435\u0442":"40702810100000001234"})
            doc_num += 1

        if random.random() < 0.3:
            dk = uuid4()
            kontr = random.choice(all_keys)
            s = Decimal(str(round(random.uniform(10000, 500000), 2)))
            receipts.append({"Ref_Key":dk,"DeletionMark":False,"Number":f"BP-PR-{doc_num:06d}","Date":day,"Posted":True,
                "\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442_Key":kontr,"\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key":org_key,
                "\u0421\u0443\u043c\u043c\u0430":s,"\u041d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435\u041f\u043b\u0430\u0442\u0435\u0436\u0430":"\u041e\u043f\u043b\u0430\u0442\u0430 \u043e\u0442 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u044f",
                "\u0412\u0438\u0434\u041f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u044f":"\u041e\u043f\u043b\u0430\u0442\u0430 \u043e\u0442 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u044f","\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439\u0421\u0447\u0435\u0442":"40702810100000001234"})
            doc_num += 1

        day += timedelta(days=1)

    print(f"  Inserting BP: {len(sales)} sales, {len(purchases)} purchases, {len(payments)} payments, {len(receipts)} receipts...")
    _bulk(conn, T["doc_real"], sales); _bulk(conn, T["doc_real_t"], sales_t)
    _bulk(conn, T["doc_post"], purchases); _bulk(conn, T["doc_post_t"], purchases_t)
    _bulk(conn, T["doc_plat"], payments); _bulk(conn, T["doc_post_rs"], receipts)
    _bulk(conn, T["reg_vz"], reg_vz)
    conn.commit()
    print("  BP seeding complete")
