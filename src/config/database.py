# database.py

import logging
from tortoise import Tortoise
from .settings import Settings

# 启用 SQL 查询日志记录
logging.basicConfig()
logging.getLogger('tortoise').setLevel(logging.DEBUG)


async def init(settings: Settings):
    await Tortoise.init(
        db_url=settings.db_url,
        modules={'models': ['src.model.pg03_open_pk_ps_cs_final']},
    )
    # await Tortoise.generate_schemas()


async def close():
    await Tortoise.close_connections()
