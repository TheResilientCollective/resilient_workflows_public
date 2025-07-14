from dagster import ConfigurableResource, Config, EnvVar, get_dagster_logger

#from dagster import Field
from pydantic import Field
import requests
from sodapy import Socrata

class SocrataResource(ConfigurableResource):
    SODA_URL: str =  Field(
         description="GLEANERIO_GRAPH_URL.")
    SODA_USERNAME: str =  Field(
         description="GLEANERIO_GRAPH_NAMESPACE.")
    SODA_PASSWORD: str =  Field(
         description="GLEANERIO_GRAPH_NAMESPACE.")

    def client(self):
       return Socrata(
            self.SODA_URL,
            "FakeAppToken",
            username=self.SODA_USERNAME,
            password=self.SODA_PASSWORD,
            timeout=10
        )
    def datasets(self):
        with  self.client() as client:
            return client.datasets()

    def get_dataset_by_id(self, dataset_id):
        with  self.client() as client:
            return client.get_all(dataset_id,content_type="json")
