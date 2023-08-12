
from tortoise.models import Model
from tortoise.fields import CharField, IntField


class Pg03OpenPkPsCsFinal(Model):
    pk_id = CharField(pk=True, max_length=255)
    area_guid = CharField(max_length=255)
    area_name = CharField(max_length=255)
    company_guid = CharField(max_length=255)
    company_name = CharField(max_length=255)
    project_guid = CharField(max_length=255)
    project_name = CharField(max_length=255)
    stage_guid = CharField(max_length=255)  # 添加对应的字段类型和选项
    stage_name = CharField(max_length=255)
    # obtain_land_date = CharField(max_length=255)
    cmsk_plan_00039_plan_finish_dt = CharField(max_length=255)
    cmsk_plan_00039_plan_finish_duration = CharField(max_length=255)
    cmsk_plan_00039_real_finish_dt = CharField(max_length=255)
    cmsk_plan_00039_real_finish_duration = CharField(max_length=255)
    cmsk_plan_00039_deviation = CharField(max_length=255)
    cmsk_plan_00040_plan_finish_dt = CharField(max_length=255)
    cmsk_plan_00040_plan_finish_duration = CharField(max_length=255)
    cmsk_plan_00040_real_finish_dt = CharField(max_length=255)
    cmsk_plan_00040_real_finish_duration = CharField(max_length=255)
    cmsk_plan_00040_deviation = CharField(max_length=255)
    cmsk_plan_00789_plan_finish_dt = CharField(max_length=255)
    cmsk_plan_00789_plan_finish_duration = CharField(max_length=255)
    cmsk_plan_00789_real_finish_dt = CharField(max_length=255)
    cmsk_plan_00789_real_finish_duration = CharField(max_length=255)
    cmsk_plan_00789_deviation = CharField(max_length=255)

    class Meta:
        table = "pg03_open_pk_ps_cs_final"

    # Defining ``__str__`` is also optional, but gives you pretty
    # represent of model in debugger and interpreter
    def __str__(self):
        return self.name


class OpenPkPsNode(Model):
    id = IntField(pk=True)
    pk_id = CharField(max_length=100)
    area_guid = CharField(max_length=100)
    area_name = CharField(max_length=100)
    company_guid = CharField(max_length=100)
    company_name = CharField(max_length=255)
    project_guid = CharField(max_length=100)
    project_name = CharField(max_length=255)
    stage_guid = CharField(max_length=100)
    stage_name = CharField(max_length=255)
    node_name = CharField(max_length=255)
    jhwc_date = CharField(max_length=100)
    jhwc_zq = CharField(max_length=100)
    yj_wc_date = CharField(max_length=255)
    sj_wc_date = CharField(max_length=100)
    sjwc_zq = CharField(max_length=255)
    wc_qk = CharField(max_length=255)

    class Meta:
        table = "open_pk_ps_node"
