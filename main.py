from functools import lru_cache
from fastapi import FastAPI
from tortoise import run_async
from src.config.settings import Settings
from src.config.database import init, close
from src.model.pg03_open_pk_ps_cs_final import Pg03OpenPkPsCsFinal

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
