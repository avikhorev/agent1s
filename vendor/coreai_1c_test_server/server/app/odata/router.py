"""FastAPI router implementing OData v3 endpoints compatible with 1C."""
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select, func

from app.database import async_session, get_tables
from app.configurations.registry import CONFIGURATIONS, get_entity_def, list_configurations, list_entities
from app.odata.parser import parse_filter, parse_select, parse_orderby
from app.odata.serializer import serialize_collection, serialize_entity, serialize_error, serialize_count
from app.odata.metadata import generate_metadata_xml

router = APIRouter()


@router.get("/")
async def root():
    return {
        "service": "1C OData Mock Server",
        "configurations": list_configurations(),
        "usage": "GET /{config}/odata/standard.odata/{EntityName}?$format=json",
    }


@router.get("/{config_name}/odata/standard.odata/$metadata")
@router.get("/{config_name}/odata/standard.odata/%24metadata")
async def odata_metadata(config_name: str, request: Request):
    cfg = CONFIGURATIONS.get(config_name)
    if cfg is None:
        raise HTTPException(404, f"Configuration \'{config_name}\' not found")
    xml = generate_metadata_xml(cfg, str(request.base_url).rstrip("/"))
    return Response(content=xml, media_type="application/xml")


@router.get("/{config_name}/odata/standard.odata")
async def odata_service_document(config_name: str):
    cfg = CONFIGURATIONS.get(config_name)
    if cfg is None:
        raise HTTPException(404, f"Configuration \'{config_name}\' not found")
    return {
        "odata.metadata": f"/{config_name}/odata/standard.odata/$metadata",
        "value": [{"name": e, "url": e} for e in list_entities(config_name)],
    }


@router.get("/{config_name}/odata/standard.odata/{entity_name}(guid\'{ref_key}\')")
async def odata_get_by_key(config_name: str, entity_name: str, ref_key: str, request: Request,
                           _select: str = Query(None, alias="$select"),
                           _format: str = Query("json", alias="$format")):
    from uuid import UUID as U
    tables = get_tables()
    ct = tables.get(config_name)
    if ct is None:
        raise HTTPException(404, "Config not found")
    table = ct.get(entity_name)
    if table is None:
        raise HTTPException(404, "Entity not found")
    base = str(request.base_url).rstrip("/")
    murl = f"{base}/{config_name}/odata/standard.odata/$metadata#{entity_name}"
    try:
        key = U(ref_key)
    except ValueError:
        return JSONResponse(status_code=400, content=serialize_error("Invalid GUID"))
    stmt = select(table).where(table.c["Ref_Key"] == key)
    async with async_session() as session:
        result = await session.execute(stmt)
        row = result.fetchone()
    if row is None:
        raise HTTPException(404, "Entity not found")
    return JSONResponse(content=serialize_entity(row, murl))


@router.get("/{config_name}/odata/standard.odata/{entity_name}")
async def odata_query(config_name: str, entity_name: str, request: Request,
                      _filter: str = Query(None, alias="$filter"),
                      _select: str = Query(None, alias="$select"),
                      _orderby: str = Query(None, alias="$orderby"),
                      _top: int = Query(None, alias="$top"),
                      _skip: int = Query(None, alias="$skip"),
                      _count: str = Query(None, alias="$count"),
                      _format: str = Query("json", alias="$format")):
    tables = get_tables()
    ct = tables.get(config_name)
    if ct is None:
        raise HTTPException(404, "Config not found")
    table = ct.get(entity_name)
    if table is None:
        raise HTTPException(404, "Entity not found")
    base = str(request.base_url).rstrip("/")
    murl = f"{base}/{config_name}/odata/standard.odata/$metadata#{entity_name}"
    try:
        sc = parse_select(_select, table)
        stmt = select(*sc) if sc else select(table)
        fc = parse_filter(_filter, table)
        if fc is not None:
            stmt = stmt.where(fc)
        oc = parse_orderby(_orderby, table)
        if oc:
            stmt = stmt.order_by(*oc)
        if _skip:
            stmt = stmt.offset(_skip)
        stmt = stmt.limit(_top if _top else 1000)
        async with async_session() as session:
            if _count and _count.lower() == "true":
                cs = select(func.count()).select_from(table)
                if fc is not None:
                    cs = cs.where(fc)
                cr = await session.execute(cs)
                return Response(content=serialize_count(cr.scalar()), media_type="text/plain")
            result = await session.execute(stmt)
            rows = result.fetchall()
        cn = [f.strip() for f in _select.split(",")] if _select else None
        return JSONResponse(content=serialize_collection(rows, murl, cn))
    except KeyError as e:
        return JSONResponse(status_code=400, content=serialize_error(f"Unknown field: {e}"))
    except Exception as e:
        return JSONResponse(status_code=400, content=serialize_error(str(e)))
