"""FastAPI entry point for the 1C OData Mock Server."""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import init_tables
from app.configurations.registry import CONFIGURATIONS
from app.odata.router import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_tables(CONFIGURATIONS)
    yield


app = FastAPI(
    title="1C OData Mock Server",
    description="Mock server emulating 1C:Enterprise OData REST API",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)
