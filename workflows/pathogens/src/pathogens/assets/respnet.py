# ruff: noqa
# We will be filling this file out as part of the PyData workshop!
import string
import pandas as pd
from odata import ODataService
from dagstermill import define_dagstermill_asset

from dagster import AssetIn, Field, Int, asset, file_relative_path

# TODO 2: Uncomment the code below to create a Dagster asset of the Iris dataset
# relevant documentation - https://docs.dagster.io/concepts/assets/software-defined-assets#a-basic-software-defined-asset

soda_string= "https://${domain}/api/odata/v4/${dataset_identifier}"
soda_template = string.Template(soda_string)
cdc_domain = "data.cdc.gov"
@asset(group_name="geospatial")
def respnet_dataset():
    return pd.read_csv(
        "https://oss.resilientservice.mooo.com/resilientdata/cdc/resp_net/Rates_of_Laboratory-Confirmed_RSV__COVID-19__and_Flu_Hospitalizations_from_the_RESP-NET_Surveillance_Systems_20240714.csv",

    )
@asset(group_name="nndss")
def respnet_download():
    # json endpoint
    respnet_identifier="n8mc-b4w4.json"
    odata_endpoint = soda_template.substitute({"domain":cdc_domain, "dataset_identifier":respnet_identifier})
    service = ODataService(odata_endpoint)

    return pd.read_csv(
        "https://oss.resilientservice.mooo.com/resilientdata/cdc/resp_net/Rates_of_Laboratory-Confirmed_RSV__COVID-19__and_Flu_Hospitalizations_from_the_RESP-NET_Surveillance_Systems_20240714.csv",

 )


# TODO 1: Uncomment the code below to create a Dagster asset backed by a Jupyter notebook
# relevant documentation - https://docs.dagster.io/_apidocs/libraries/dagstermill#dagstermill.define_dagstermill_asset

respnet_jupyter_notebook = define_dagstermill_asset(
    name="respnet_jupyter",
    notebook_path=file_relative_path(__file__, "../../notebooks/RESP-NET.ipynb"),
    group_name="geospatial",
    # ins={"iris": AssetIn("respnet_dataset")},  # this code to remain commented until TODO 3
)
