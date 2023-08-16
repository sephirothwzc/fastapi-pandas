from typing import Generator
from fastapi import FastAPI, Depends
from fastapi.responses import StreamingResponse
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.service.settings import Settings
from src.service.data_pandas import data_demo, pivot_v_stage_product_constitute, unpivotPg03OpenPkPsCsFinal
import io

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


@app.get("/view")
async def pivot_v_stage(project_guid: str, stage_guid: str, session: AsyncSession = Depends(get_async_session)):
    return await pivot_v_stage_product_constitute(session, project_guid, stage_guid)


@app.get("/xlsx")
async def xlsx_response(session: AsyncSession = Depends(get_async_session)):
    # 将 DataFrame 写入 BytesIO 流
    output = io.BytesIO()
    df = data_demo(session)
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)

    # 配置 BytesIO 流
    output.seek(0)
    response = StreamingResponse(iter(output.read(
    )), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response.headers["Content-Disposition"] = "attachment; filename=output.xlsx"
    return response
