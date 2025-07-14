
html
https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services/CalEnviroScreen_4_Indicator_Tract_Data/FeatureServer/0//query?where=&objectIds=&geometry=-117.593832%2C32.419090%2C-116.599569%2C33.327941&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=true&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=4326&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=html&token=
geojson:
https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services/CalEnviroScreen_4_Indicator_Tract_Data/FeatureServer/0//query?where=&objectIds=&geometry=-117.593832%2C32.419090%2C-116.599569%2C33.327941&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=true&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=4326&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token=

Hi Dave,
Here is how to grab pesticides, and other data from CalEnviroScreen 4.
 
Also, see their web site Download Data - OEHHA points to San Ysidro air study.
 https://oehha.ca.gov/calenviroscreen/download-data
i.
 
 
Here is the code – just tested:
 
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor
gis = GIS()
 
serviceURL =  'https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services/CalEnviroScreen_4_Indicator_Tract_Data/FeatureServer/'
item = FeatureLayerCollection(serviceURL, gis=gis)
sedf = pd.DataFrame.spatial.from_layer(item.layers[0])
 
sedf_without_geometry = sedf.drop(columns=['SHAPE'])
sedf_without_geometry.to_csv("env_by_tracts.csv")
 
Also, check out other services at https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services – they may have useful data.
 

Hi Ilya,
 
Thanks so much for looking into this. If it's not much trouble, could you send me Python code to extract the table into a df?
 
-Peter
 
On Wed, Jan 15, 2025 at 6:54 PM Zaslavsky, Ilya <izaslavsky@ucsd.edu> wrote:
Hi Peter, glad to hear from you, and Happy New Year!
 
On the web site, they point to https://oehha.ca.gov/calenviroscreen/download-data
 
That page references this service:
https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services/CalEnviroScreen_4_0_Results_/FeatureServer
 
However, they point to a wrong service!! It only has a subset of the data.
 
I just found that they actually query for pop-ups this service:
 
https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis/rest/services/CalEnviroScreen_4_Indicator_Tract_Data/FeatureServer/0/
 
You can look at it at https://www.arcgis.com/apps/mapviewer/index.html?url=https://services1.arcgis.com/PCHfdHz4GlDNAhBb/ArcGIS/rest/services/CalEnviroScreen_4_Indicator_Tract_Data/FeatureServer/0/&source=sd
 
In particular, towards the end of the data table, you will find five fields that show the names of the five pesticides.
 
Let me know if you want Python code about how to grab this into a dataframe.
 
Best,
ilya
 
 
 
From: Peter Rose <pwrose.ucsd@gmail.com>
Sent: Wednesday, January 15, 2025 5:43 PM
To: Zaslavsky, Ilya <izaslavsky@ucsd.edu>
Subject: ArcGIS question
 
Hi Ilya,
 
Happy New Year! I hope you are doing well.
 
I'm still working with the UCSF folks on their KG.
 
We want to map pesticide usage to specific locations.
 
OEHHA has an ArcGIS application to look up pesticides used within a census tract.
https://oehha.maps.arcgis.com/apps/instant/sidebar/index.html?appid=99c13afc88a24ae1ace5cc5f081265eb
 
If you click on a specific region, it displays details, including a list of pesticides:
image001.png
 
We cannot figure out how to get this list of pesticides for a census tract. I assume the app is using some ArcGIS webservice to get the data. Do you have any suggestion on how we might get the list of pesticides given a census tract id?
 
Thanks,
Peter
 


--
Peter Rose, Ph.D.
Director, Structural Bioinformatics Laboratory
Lead, Bioinformatics and Biomedical Applications
Faculty Affiliate, Halıcıoğlu Data Science Institute
San Diego Supercomputer Center
UC San Diego
