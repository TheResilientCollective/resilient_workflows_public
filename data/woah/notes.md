# World organization for animal health 
https://wahis.woah.org/#/home

# Exceptional Events
Public interface:
https://wahis.woah.org/#/event-management

Data is on a sharepoint:
authorized user is: dwvalentine@ucsd.edu
https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FPeriodicaldataextractionsOIE%2DWAHIS%2FShared%20Documents
https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS

Want to grab the latest: infur_{date}.xlsx
when the metadata is updated Metadata_WeeklyExtraction.xlsx 

example sharpoint download copy link
https://oieoffice365.sharepoint.com/:x:/r/sites/PeriodicaldataextractionsOIE-WAHIS/Shared%20Documents/Metadata_WeeklyExtraction.xlsx?d=wea5e430f060e4de6bef361042848643b&csf=1&web=1&e=jYb1fF


Attempts to use scripts in scripts/sharepoint

Coding for sharepoint:
Office365-REST-Python-Client

import os
from dagster import asset
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

@asset
def latest_sharepoint_dataset():
    """
    Downloads the most recently modified file from a SharePoint directory.

    This asset connects to a SharePoint site, finds the most recent file
    in a specified folder, and downloads it locally.

    Required environment variables:
    - SHAREPOINT_URL: The URL of the SharePoint site.
    - SHAREPOINT_USER: The username for authentication.
    - SHAREPOINT_PASSWORD: The password for authentication.
    """
    sharepoint_url = os.getenv("SHAREPOINT_URL")
    sharepoint_user = os.getenv("SHAREPOINT_USER")
    sharepoint_password = os.getenv("SHAREPOINT_PASSWORD")
    
    # This is the path to your folder on SharePoint
    # You can find this in the browser URL when you navigate to the folder.
    folder_server_relative_url = "/sites/YourSite/Shared%20Documents/YourFolder"

    if not all([sharepoint_url, sharepoint_user, sharepoint_password]):
        raise ValueError(
            "Please set SHAREPOINT_URL, SHAREPOINT_USER, and "
            "SHAREPOINT_PASSWORD environment variables."
        )

    # Authenticate and create a client context
    ctx = ClientContext(sharepoint_url).with_credentials(
        UserCredential(sharepoint_user, sharepoint_password)
    )

    # Get the folder from SharePoint
    folder = ctx.web.get_folder_by_server_relative_url(folder_server_relative_url)
    files = folder.files.get().execute_query()

    if not files:
        raise FileNotFoundError(f"No files found in SharePoint folder: {folder_server_relative_url}")

    # Find the most recently modified file
    latest_file = sorted(files, key=lambda f: f.properties["TimeLastModified"], reverse=True)[0]
    latest_file_name = latest_file.properties["Name"]
    print(f"Found latest file: {latest_file_name}")

    # Define a local path to download the file to
    download_path = os.path.join("data", latest_file_name)
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    # Download the file
    with open(download_path, "wb") as local_file:
        latest_file.download(local_file).execute_query()

    print(f"File downloaded to: {download_path}")

    return download_path

import os
from datetime import datetime, timedelta
from dagster import asset
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

@asset
def updated_sharepoint_files():
    """
    Downloads files from a SharePoint directory that have been updated
    since a specific date.
    """
    sharepoint_url = os.getenv("SHAREPOINT_URL")
    sharepoint_user = os.getenv("SHAREPOINT_USER")
    sharepoint_password = os.getenv("SHAREPOINT_PASSWORD")
    
    folder_server_relative_url = "/sites/YourSite/Shared%20Documents/YourFolder"

    if not all([sharepoint_url, sharepoint_user, sharepoint_password]):
        raise ValueError(
            "Please set SHAREPOINT_URL, SHAREPOINT_USER, and "
            "SHAREPOINT_PASSWORD environment variables."
        )

    ctx = ClientContext(sharepoint_url).with_credentials(
        UserCredential(sharepoint_user, sharepoint_password)
    )

    folder = ctx.web.get_folder_by_server_relative_url(folder_server_relative_url)

    # --- Filtering Logic ---
    # You can change this to a specific date.
    # For example: since_date = datetime(2023, 10, 26)
    since_date = datetime.now() - timedelta(days=7)
    
    # SharePoint expects an ISO 8601 format with a 'Z' for UTC.
    since_date_iso = since_date.isoformat() + "Z"

    # Use an OData filter query to get files modified after the specified date.
    # The key here is the `filter()` call.
    files = folder.files.filter(f"TimeLastModified gt datetime'{since_date_iso}'").get().execute_query()

    if not files:
        print(f"No files found that were updated since {since_date.date()}.")
        return []

    downloaded_paths = []
    print(f"Found {len(files)} updated files:")
    for f in files:
        file_name = f.properties["Name"]
        print(f"- {file_name} (Last Modified: {f.properties['TimeLastModified']})")

        # Download each updated file
        download_path = os.path.join("data", "sharepoint_updates", file_name)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        with open(download_path, "wb") as local_file:
            f.download(local_file).execute_query()

        print(f"  Downloaded to: {download_path}")
        downloaded_paths.append(download_path)

    return downloaded_paths
