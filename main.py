from typing import Generator
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.service.settings import Settings
from src.service.data_pandas import unpivotPg03OpenPkPsCsFinal

app = FastAPI()

# 创建异步数据库引擎
async_engine = create_async_engine(
    Settings().db_url, echo=True)  # 添加 echo=True 以便查看 SQL 输出

# 创建异步数据库 Session 类
AsyncSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=async_engine, class_=AsyncSession)

# 创建异步数据库 Session 类


async def get_async_session() -> Generator[AsyncSession, None, None]:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            yield session

# 示例的异步服务函数


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/pg03")
async def pg03(session: AsyncSession = Depends(get_async_session)):
    return await unpivotPg03OpenPkPsCsFinal(session)
