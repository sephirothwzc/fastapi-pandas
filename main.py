from functools import lru_cache
import json
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
from src.config.models import OpenPkPsNode, Pg03OpenPkPsCsFinal
from src.config.settings import Settings
from src.config.database import init, close
from src.service.data_pandas import unpivot_columns, unpivotPg03OpenPkPsCsFinal

app = FastAPI()


@lru_cache()
def get_settings():
    return Settings()


@app.on_event("startup")
async def startup_db():
    await init(get_settings())


@app.on_event("shutdown")
async def shutdown_db():
    await close()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/demo")
async def demo():
    list = await Pg03OpenPkPsCsFinal.all().values()
    return list


@app.get("/demo2")
async def demo2():
    return await unpivotPg03OpenPkPsCsFinal()
