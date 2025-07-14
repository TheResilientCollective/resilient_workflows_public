from foursquare.data_sdk import DataSDK, MediaType
from dagster import ConfigurableResource, Field
from contextlib import contextmanager
class FoursquareClient():
    client=None
    def __init__(self, refresh_token=None):
        self.client = DataSDK(refresh_token=refresh_token)
    def listDatasets(self):
        return self.client.list_datasets()

    def updateDataset(self, dataset_id, csv, dataset):
        #file: BinaryIO | str | Path | None = None,
        self.client.update_dataset(
            dataset,
            name='New name',
        description = 'New description',
        file = 'new_file.csv',
        media_type = MediaType.CSV
        )
    def updateDatasetByDataframe(self, dataset_id, dataframe, dataset,
                                 name='Dataset name',description='Dataset description' ):
        self.client.upload_dataframe(
            dataframe,
            dataset=dataset,
            name=name,
            description=description)
class FoursquareResource(ConfigurableResource):
    RC_FSQ_REFRESH_TOKEN: str = Field(
        description="Foursuare refresh token from https://docs.foursquare.com/developer/docs/studio-authentication",
        default_value=None)

    def create_resource(self) -> FoursquareClient:
        return  FoursquareClient(refresh_token=self.RC_FSQ_REFRESH_TOKEN)
