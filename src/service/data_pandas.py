# data_pandas.py

import json
from typing import Any, List, Dict
import pandas as pd

from src.config.models import Pg03OpenPkPsCsFinal


key_mapping = {
    "00039": "获取土地",
    "00040": "项目团队关键岗位确定",
    "00789": "投资交底会"
}


def replace_functions(value: str):
    for key, replacement in key_mapping.items():
        if value.find(key) != -1:
            return replacement

    return value


async def unpivotPg03OpenPkPsCsFinal():
    data_list = await Pg03OpenPkPsCsFinal.all().values()

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
    plan_df = unpivot_columns(
        data_list, id_vars, plan_columns, "计划完成时间", "类型")

    # row_replace(plan_df)
    plan_df["类型"] = plan_df["类型"].apply(replace_functions)

    actual_df = unpivot_columns(
        data_list, id_other, actual_columns, "实际完成时间", "类型")

    # row_replace(actual_df)
    actual_df["类型"] = actual_df["类型"].apply(replace_functions)

    duration_df = unpivot_columns(
        data_list, id_other, duration_columns, "计划完成绝对工期", "类型")

    # row_replace(duration_df)
    duration_df["类型"] = duration_df["类型"].apply(replace_functions)

    real_duration_df = unpivot_columns(
        data_list, id_other, real_duration_columns, "实际完成绝对工期", "类型")

    # row_replace(real_duration_df)
    real_duration_df["类型"] = real_duration_df["类型"].apply(replace_functions)

    deviation_df = unpivot_columns(
        data_list, id_other, deviation_columns, "偏差", "类型")

    # row_replace(deviation_df)
    deviation_df["类型"] = deviation_df["类型"].apply(replace_functions)
    # endregion

    # # 合并两个DataFrame
    merged_df = pd.merge(plan_df, actual_df, on=id_other + ["类型"])
    merged_df = pd.merge(merged_df, duration_df, on=id_other + ["类型"])
    merged_df = pd.merge(merged_df, real_duration_df, on=id_other + ["类型"])
    merged_df = pd.merge(merged_df, deviation_df, on=id_other + ["类型"])

    # 计算完成情况列
    merged_df["完成情况"] = (pd.to_datetime(merged_df["实际完成时间"], format="mixed") -
                         pd.to_datetime(merged_df["计划完成时间"], format="mixed")).dt.days

    # 将合并后的DataFrame转换成JSON
    result_json = merged_df.to_json(orient="records")

    return json.loads(result_json)


# 行列转换
def unpivot_columns(data_list: List[Dict[str, Any]], id_vars: List[str], columns: List[str],
                    target_name: str, var_name: str) -> pd.DataFrame:

    df = pd.DataFrame.from_records(data_list)

    unpivoted_df = pd.melt(df, id_vars=id_vars,
                           value_vars=columns,
                           var_name=var_name, value_name=target_name)

    return unpivoted_df
