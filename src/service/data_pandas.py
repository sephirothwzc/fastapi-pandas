# data_pandas.py

import json
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from src.service.utils import insert_data, pivot_columns, pivot_table_columns, replace_functions, sql_to_data, unpivot_columns

from src.service.snowflake import IdWorker


# 创建一个 雪花id
worker = IdWorker(1, 2, 0)

key_mapping = {
    "00039": "获取土地",
    "00040": "项目团队关键岗位确定",
    "00789": "投资交底会"
}


async def unpivotPg03OpenPkPsCsFinal(session: AsyncSession):
    """
    从数据库查询指定项目和阶段的产品构成数据，并进行行转列操作。

    :param session: 数据库会话对象（异步会话）。
    :return: json
    """
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

    # 使用 replace_functions 函数对 "类型" 列的值进行替换
    plan_df["task_name"] = plan_df["task_name"].apply(
        lambda value: replace_functions(value, key_mapping))

    # 实际完成时间
    actual_df = unpivot_columns(
        data_list, id_other, actual_columns, "real_finish_dt", "task_name")

    actual_df["task_name"] = actual_df["task_name"].apply(
        lambda value: replace_functions(value, key_mapping))

    # 计划完成绝对工期
    duration_df = unpivot_columns(
        data_list, id_other, duration_columns, "plan_finish_duration", "task_name")

    duration_df["task_name"] = duration_df["task_name"].apply(
        lambda value: replace_functions(value, key_mapping))
    # 实际完成绝对工期
    real_duration_df = unpivot_columns(
        data_list, id_other, real_duration_columns, "real_finish_duration", "task_name")

    real_duration_df["task_name"] = real_duration_df["task_name"].apply(
        lambda value: replace_functions(value, key_mapping))
    # 偏差
    deviation_df = unpivot_columns(
        data_list, id_other, deviation_columns, "deviation", "task_name")

    deviation_df["task_name"] = deviation_df["task_name"].apply(
        lambda value: replace_functions(value, key_mapping))
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


async def pivot_v_stage_product_constitute(session: AsyncSession, project_guid: str, stage_guid: str):
    """
    行转列

    从数据库查询指定项目和阶段的产品构成数据，并进行行转列操作。

    :param session: 数据库会话对象（异步会话）。
    :param project_guid: 项目的全局唯一标识符。
    :param stage_guid: 阶段的全局唯一标识符。
    :return: 转换后的数据列表，包含产品构成数据的行转列结果。
    """
    query_sql = "SELECT * FROM v_stage_product_constitute WHERE project_guid = :project_guid AND stage_guid = :stage_guid"
    params = {"project_guid": project_guid, "stage_guid": stage_guid}
    data_list = await sql_to_data(query_sql, session, params)

    id_vars = ["project_guid"]
    columns = ["secondaryproduct_type"]
    values = ["mj", "ts", "totalvalue", "wshzjj"]

    # 行转列 生成二维表头 不重构索引
    mj_df = pivot_table_columns(data_list=data_list, id_vars=id_vars,
                                columns=columns, values=values)

    # 宽表变长表 stack 降级 重构索引
    mj_df = mj_df.stack(level=0).reset_index()
    # 列转行
    # ========

    print(mj_df)
    # 将合并后的DataFrame转换成JSON
    result_json = mj_df.to_json(orient="records")

    return json.loads(result_json)


async def data_demo(session: AsyncSession) -> pd.DataFrame:
    query = "SELECT * FROM pg03_open_pk_ps_cs_final"
    data_list = await sql_to_data(query, session)
    df = pd.DataFrame(data_list)
    return df
