from dagster import asset
from ..resources.arcgis import getGeojson
# arcgiso onelin: "https://{host}/{org_id}/arcgis/rest/services/{layer_name}/FeatureServer/{layer_id}/query"

# sewer_main https://geo.sandag.org/server/rest/services/Hosted/Sewer_Main_SD/FeatureServer
@asset("sewer_main_sd")
def get_plumbing():
    getGeojson("geo.sandag.org", None, "Hosted/Sewer_Main_SD", "0",s3bucket="public", s3path="spatial", auth=None)
