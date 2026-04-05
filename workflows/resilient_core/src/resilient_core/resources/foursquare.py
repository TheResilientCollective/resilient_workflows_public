from dagster import (
    asset, get_dagster_logger, define_asset_job, ConfigurableResource)
from foursquare.data_sdk import DataSDK, MediaType
import os
from pydantic import Field,ConfigDict
refresh_token=os.getenv("RC_FSQ_REFRESH_TOKEN")

class ResourceWithFourSqaureConfiguration(ConfigurableResource):

    # this should be s3, since it is the s3 resource. Others at gleaner s3 resources
    RC_FSQ_REFRESH_TOKEN: str =  Field(
         description="S3_BUCKET.")

class FourSqaureResource(ResourceWithFourSqaureConfiguration):

     def getClient(self) -> DataSDK:
         try:
             return DataSDK(
                 refresh_token=self.RC_FSQ_REFRESH_TOKEN,
             )
         except:
            return DataSDK(
            )
