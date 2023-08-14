# data_pandas.py

import json
from typing import Any, List, Dict
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.service.snowflake import IdWorker


# 创建一个 雪花id
worker = IdWorker(1, 2, 0)

key_mapping = {
    "00039": "获取土地",
    "00040": "项目团队关键岗位确定",
    "00789": "投资交底会"
}


# 行列转换
async def unpivotPg03OpenPkPsCsFinal(session: AsyncSession):
    query = "SELECT * FROM pg03_open_pk_ps_cs_final"
    data_list = await sql_to_data(query, session)

    # 定义 id_vars、计划列和实际列
    id_vars = ["pk_id", "area_guid", "area_name", "company_guid",
               "company_name", "project_guid", "project_name", "stage_name", "stage_guid"]
    id_other = ["pk_id"]
    # region 转换行
    # 计划完成时间
    plan_columns = ["cmsk_plan_00039_plan_finish_dt",
                    "cmsk_plan_00040_plan_finish_dt",
                    "cmsk_plan_00789_plan_finish_dt"]

    # 实际完成时间
    actual_columns = ["cmsk_plan_00039_real_finish_dt",
                      "cmsk_plan_00040_real_finish_dt",
                      "cmsk_plan_00789_real_finish_dt"]

    # 计划完成绝对工期
    duration_columns = ["cmsk_plan_00039_plan_finish_duration",
                        "cmsk_plan_00040_plan_finish_duration",
                        "cmsk_plan_00789_plan_finish_duration"]

    # 实际完成绝对工期
    real_duration_columns = ["cmsk_plan_00039_real_finish_duration",
                             "cmsk_plan_00040_real_finish_duration",
                             "cmsk_plan_00789_real_finish_duration"]

    # 偏差
    deviation_columns = ["cmsk_plan_00039_deviation",
                         "cmsk_plan_00040_deviation",
                         "cmsk_plan_00789_deviation"]
    # endregion

    # region unpivot_columns
    # 调用函数进行转换并合并
    # 计划完成时间
    plan_df = unpivot_columns(
        data_list, id_vars, plan_columns, "plan_finish_dt", "task_name")

    plan_df["task_name"] = plan_df["task_name"].apply(replace_functions)

    # 实际完成时间
    actual_df = unpivot_columns(
        data_list, id_other, actual_columns, "real_finish_dt", "task_name")

    actual_df["task_name"] = actual_df["task_name"].apply(replace_functions)

    # 计划完成绝对工期
    duration_df = unpivot_columns(
        data_list, id_other, duration_columns, "plan_finish_duration", "task_name")

    duration_df["task_name"] = duration_df["task_name"].apply(
        replace_functions)
    # 实际完成绝对工期
    real_duration_df = unpivot_columns(
        data_list, id_other, real_duration_columns, "real_finish_duration", "task_name")

    real_duration_df["task_name"] = real_duration_df["task_name"].apply(
        replace_functions)
    # 偏差
    deviation_df = unpivot_columns(
        data_list, id_other, deviation_columns, "deviation", "task_name")

    deviation_df["task_name"] = deviation_df["task_name"].apply(
        replace_functions)
    # endregion

    # # 合并两个DataFrame
    merged_df = pd.merge(plan_df, actual_df, on=id_other + ["task_name"])
    merged_df = pd.merge(merged_df, duration_df, on=id_other + ["task_name"])
    merged_df = pd.merge(merged_df, real_duration_df,
                         on=id_other + ["task_name"])
    merged_df = pd.merge(merged_df, deviation_df, on=id_other + ["task_name"])

    # 计算完成情况列
    merged_df["completion_status"] = (pd.to_datetime(merged_df["real_finish_dt"], format="mixed") -
                                      pd.to_datetime(merged_df["plan_finish_dt"], format="mixed")).dt.days

    # 重命名列
    # merged_df.rename(columns={"pk_id": "id"}, inplace=True)
    merged_df['id'] = merged_df.apply(lambda row: worker.get_id(), axis=1)

    # 数据异步插入
    await insert_data("p_stage_open_pk_node", merged_df, session)

    # 将合并后的DataFrame转换成JSON
    result_json = merged_df.to_json(orient="records")

    return json.loads(result_json)


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

# 行列转换


def unpivot_columns(data_list: List[Dict[str, Any]], id_vars: List[str], columns: List[str],
                    target_name: str, var_name: str) -> pd.DataFrame:

    df = pd.DataFrame.from_records(data_list)

    unpivoted_df = pd.melt(df, id_vars=id_vars,
                           value_vars=columns,
                           var_name=var_name, value_name=target_name)

    return unpivoted_df

# 替换映射


def replace_functions(value: str):
    for key, replacement in key_mapping.items():
        if value.find(key) != -1:
            return replacement

    return value

# endregion
