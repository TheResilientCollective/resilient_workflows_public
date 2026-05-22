from dagster import asset
from requests import request

@asset()
def plants(context):
    pass

def plant_data(context):
    pass
