# utils.py

from typing import Any, Dict, List
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# region 公共方法

# 异步查询sql


async def sql_to_data(sqlQuery: str, session: AsyncSession):
    result = await session.execute(text(sqlQuery))
    rows = result.fetchall()

    # 将结果转换为列表
    data = [row._asdict() for row in rows]

    return data

# 异步插入数据


async def insert_data(table_name: str, data_df, session: AsyncSession):
    for _, row in data_df.iterrows():
        values = ", ".join([f":{col}" for col in data_df.columns])
        insert_statement = text(
            f"INSERT INTO {table_name} ({', '.join(data_df.columns)}) VALUES ({values})")
        await session.execute(insert_statement, params=row.to_dict())


def unpivot_columns(data_list: List[Dict[str, Any]], id_vars: List[str], columns: List[str],
                    target_name: str, var_name: str) -> pd.DataFrame:

    df = pd.DataFrame.from_records(data_list)

    unpivoted_df = pd.melt(df, id_vars=id_vars,
                           value_vars=columns,
                           var_name=var_name, value_name=target_name)

    return unpivoted_df

# 替换映射


def replace_functions(value: str, key_mapping):
    for key, replacement in key_mapping.items():
        if value.find(key) != -1:
            return replacement

    return value

# endregion
