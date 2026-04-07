
import requests
import json
from pyproj import Transformer
import minio

def getGeojson(host, org_id,layer_name,layer_id, s3bucket="public", s3path="public", auth=None):

    if org_id is None:
        # https://geo.sandag.org/server/rest/services/Hosted/Sewer_Main_SD/FeatureServer
        url = "https://{host}/server/rest/services/{layer_name}/FeatureServer/{layer_id}/query"
    else:
        url = "https://{host}/{org_id}/arcgis/rest/services/{layer_name}/FeatureServer/{layer_id}/query"
    params = {
        'where': '1=1',
        'outSR': '4326',  # Request WGS84
        'f': 'geojson'
    }

    # Step 2: Request the data
    response = requests.get(url, params=params)
    geojson_data = response.json()

    # Step 3: Convert CRS to WGS84 (if needed, here it's not needed as outSR=4326)
    # If transformation was needed, you could use pyproj like this:
    # transformer = Transformer.from_crs("EPSG:your_input_crs", "EPSG:4326")

    # You can save the GeoJSON or process it further
    with open('data_wgs84.geojson', 'w') as f:
        json.dump(geojson_data, f)

    print("GeoJSON data saved as data_wgs84.geojson")

