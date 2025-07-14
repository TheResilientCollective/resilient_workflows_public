import os

import pandas as pd
from pyairtable import Api, Table
from pyairtable.models.schema import  FieldSchema, TableSchema
from pydantic import Field,ConfigDict
from dagster import asset, get_dagster_logger, define_asset_job, ConfigurableResource
class ResourceWithAirtableConfiguration(ConfigurableResource):

    # this should be s3, since it is the s3 resource. Others at gleaner s3 resources
    AIRTABLE_ACCESS_TOKEN: str =  Field(
         description="AIRTABLE_ACCESS_TOKEN")
    AIRTABLE_BASE_ID: str =  Field(
         description="AIRTABLE_BASE_ID")


class AirtableResource( ResourceWithAirtableConfiguration):

    def getClient(self) -> Api:
        return Api(self.AIRTABLE_ACCESS_TOKEN)

    def getTable(self, tableid) -> Table:
        return self.getClient().table(self.AIRTABLE_BASE_ID, tableid)

    def getTableFields(self, tableid) -> list[FieldSchema]:
        return self.getClient().table(self.AIRTABLE_BASE_ID, tableid).schema().fields
    def upsert2Table(self, tableid: str, df: pd.DataFrame, keyfields=None) -> list:
        records = []
        get_dagster_logger().info(f"airtable  dataframe types: {df.dtypes}")
        df.fillna('', inplace=True)
        for key, row in df.iterrows():
            fields = row.to_dict()
            get_dagster_logger().debug(fields)
            arecord = { 'fields': fields }
            records.append(arecord)
        #get_dagster_logger().info(records)
        ids=self.getTable(tableid).batch_upsert(records,keyfields, typecast=True)
        return ids
