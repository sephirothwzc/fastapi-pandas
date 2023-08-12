from tortoise.models import Model
from tortoise import fields


class Pg03OpenPkPsCsFinal(Model):
    # Defining `id` field is optional, it will be defined automatically
    # if you haven't done it yourself
    pk_id = fields.IntField(pk=True)
    area_guid = fields.CharField(max_length=255)
    area_name = fields.CharField(max_length=255)
    company_guid = fields.CharField(max_length=255)
    company_name = fields.CharField(max_length=255)
    project_guid = fields.CharField(max_length=255)
    project_name = fields.CharField(max_length=255)
    stage_guid = fields.CharField(max_length=255)
    stage_name = fields.CharField(max_length=255)

    class Meta:
        table = "pg03_open_pk_ps_cs_final"

    # Defining ``__str__`` is also optional, but gives you pretty
    # represent of model in debugger and interpreter
    def __str__(self):
        return self.name
