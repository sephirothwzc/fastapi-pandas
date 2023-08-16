# utils.py

from typing import Any, Dict, List, Union
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# region 公共方法

# 异步查询sql


async def sql_to_data(sqlQuery: str, session: AsyncSession, params: Any = None) -> List[Dict[str, Any]]:
    """异步查询sql

    Args:
        sqlQuery (str): _description_
        session (AsyncSession): _description_
        params (Any): _description_

    Returns:
        List[Dict[str, Any]]: _description_
    """
    result = await session.execute(text(sqlQuery), params)
    rows = result.fetchall()

    # 将结果转换为列表
    data = [row._asdict() for row in rows]

    return data


async def insert_data(table_name: str, data_df, session: AsyncSession):
    """
    异步插入数据

    Args:
        table_name (str): 表名
        data_df (_type_): pandas.dataformat
        session (AsyncSession): 数据库连接
    """
    for _, row in data_df.iterrows():
        values = ", ".join([f":{col}" for col in data_df.columns])
        insert_statement = text(
            f"INSERT INTO {table_name} ({', '.join(data_df.columns)}) VALUES ({values})")
        await session.execute(insert_statement, params=row.to_dict())


def unpivot_columns(data_list: List[Dict[str, Any]], id_vars: List[str], columns: List[str],
                    target_name: str, var_name: str) -> pd.DataFrame:
    """
    列转行

    Args:
        data_list (List[Dict[str, Any]]): _description_
        id_vars (List[str]): _description_
        columns (List[str]): _description_
        target_name (str): _description_
        var_name (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = pd.DataFrame.from_records(data_list)

    unpivoted_df = pd.melt(df, id_vars=id_vars,
                           value_vars=columns,
                           var_name=var_name, value_name=target_name)

    return unpivoted_df


def pivot_table_columns(data_list: List[Dict[str, Any]], id_vars: Union[str, List[str]], columns: Union[str, List[str]], values: Union[str, List[str]] = None, aggfunc: str = "mean", fill_value=0):
    """
    行转列 聚合数据

    Args:
        data_list (List[Dict[str, Any]]): 数据源，就是要分析的DataFrame对象
        id_vars (Union[str, List[str]]): 行层次的分组键，它可以是一个值，也可以是多个值，如果是多个值，则需要用列表的方括号括起来，从而形成多级索引。
        values (Union[str, List[str]]): 指定要就要的数据字段，默认为全部数值型数据
        columns (Union[str, List[str]]): 列层次的分组键，在理解上和index类似
        aggfunc (str): 对数据执行聚合操作时所用的函数。当我们未设置aggfunc时，默认aggfunc='mean'，表明计算均值。

    Returns:
        _type_: _description_
    """
    df = pd.DataFrame.from_records(data_list)

    pivot_df = pd.pivot_table(
        df, values=values, index=id_vars, columns=columns, aggfunc=aggfunc, fill_value=fill_value)
    return pivot_df


def pivot_columns(data_list: List[Dict[str, Any]], id_vars: Union[str, List[str]], columns: Union[str, List[str]], values: Union[str, List[str]] = None, fill_value=0):
    """
    行转列 聚合数据

    Args:
        data_list (List[Dict[str, Any]]): 数据源，就是要分析的DataFrame对象
        id_vars (Union[str, List[str]]): 行层次的分组键，它可以是一个值，也可以是多个值，如果是多个值，则需要用列表的方括号括起来，从而形成多级索引。
        values (Union[str, List[str]]): 指定要就要的数据字段，默认为全部数值型数据
        columns (Union[str, List[str]]): 列层次的分组键，在理解上和index类似

    Returns:
        _type_: _description_
    """
    df = pd.DataFrame.from_records(data_list)

    pivot_df = pd.pivot(
        df, index=id_vars, columns=columns, values=values)
    return pivot_df


def replace_functions(value: str, key_mapping) -> str:
    """
    替换映射

    Args:
        value (str): _description_
        key_mapping (_type_): _description_

    Returns:
        str: _description_
    """
    for key, replacement in key_mapping.items():
        if value.find(key) != -1:
            return replacement

    return value

# endregion
