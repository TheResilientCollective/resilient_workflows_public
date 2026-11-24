from dagster import Config

# The workbook reader lives in resilient_core so it can be used without Dagster.
# Re-exported here for backwards compatibility.
from resilient_core.utils.tableau_workbook import (  # noqa: F401
    TableauWorkbookProcessor,
    convert_tableau_timestamps_to_datetime,
)


class TableauWastewaterConfig(Config):
    # urls from the website
    # the website at present "https://public.tableau.com/app/profile/epidemiology.immunization.services.branch/viz/DraftRespDash_750/RespDash_750"
    # the download url from the download button: "Request URL: https://public.tableau.com/workbooks/DraftRespDash_750.twb"
   # url: str = "https://public.tableau.com/workbooks/DraftRespDash.twb"
    url: str = "https://public.tableau.com/workbooks/SDPHL_WW_ddPCR.twb"
    workbook_name: str = "sandiego_wastewater"
    wb_api_url: str = "https://public.tableau.com/profile/api/single_workbook/SDPHL_WW_ddPCR"

class TableauWorkbookConfig(Config):
    # urls from the website
    # the website at present "https://public.tableau.com/app/profile/epidemiology.immunization.services.branch/viz/DraftRespDash_750/RespDash_750"
    # the download url from the download button: "Request URL: https://public.tableau.com/workbooks/DraftRespDash_750.twb"
   # url: str = "https://public.tableau.com/workbooks/DraftRespDash.twb"
    url: str = "https://public.tableau.com/workbooks/DraftRespDash_750.twb"
    workbook_name: str = "sandiego_epidemiology"
    wb_api_url: str = "https://public.tableau.com/profile/api/single_workbook/DraftRespDash_750"
