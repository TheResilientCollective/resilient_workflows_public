import os
import io
from dagster import asset
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from pydantic import Field, PrivateAttr
from dagster import get_dagster_logger, ConfigurableResource

class SharepointResource(ConfigurableResource):
    SHAREPOINT_URL: str = Field(
        default="https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/",
        description="SharePoint Site URL"
    )

    # Certificate Authentication (preferred)
    SHAREPOINT_CLIENT_ID: Optional[str] = Field(
        default=None,
        description="Azure AD Application Client ID"
    )
    SHAREPOINT_TENANT_ID: Optional[str] = Field(
        default=None,
        description="Azure AD Tenant ID"
    )
    SHAREPOINT_CERTIFICATE_PATH: Optional[str] = Field(
        default=None,
        description="Path to certificate.pem file"
    )
    SHAREPOINT_PRIVATE_KEY_PATH: Optional[str] = Field(
        default=None,
        description="Path to private_key.pem file"
    )
    SHAREPOINT_THUMBPRINT: Optional[str] = Field(
        default=None,
        description="Certificate thumbprint (SHA1 fingerprint without colons)"
    )

    # Legacy User/Password Authentication (deprecated)
    SHAREPOINT_USER: Optional[str] = Field(
        default=None,
        description="SharePoint User (deprecated - use certificate auth)"
    )
    SHAREPOINT_PASSWORD: Optional[str] = Field(
        default=None,
        description="SharePoint Password (deprecated - use certificate auth)"
    )

    _ctx: Optional[Any] = PrivateAttr(default=None)

    def authenticate(self):
        """Authenticate to SharePoint using certificate (preferred) or username/password (fallback)"""
        if self._ctx is not None:
            return self._ctx

        logger = get_dagster_logger()

        # Try certificate authentication first (preferred method)
        if all([self.SHAREPOINT_CLIENT_ID, self.SHAREPOINT_THUMBPRINT]):
            logger.info("Using certificate authentication for SharePoint")
            try:
                # Read the private key content
                if self.SHAREPOINT_PRIVATE_KEY_PATH and os.path.exists(self.SHAREPOINT_PRIVATE_KEY_PATH):
                    with open(self.SHAREPOINT_PRIVATE_KEY_PATH, 'r') as f:
                        private_key_content = f.read()
                else:
                    raise ValueError("Private key file not found or not specified")

                # office365-rest-python-client with keyword arguments
                self._ctx = ClientContext(self.SHAREPOINT_URL).with_client_certificate(
                    self.SHAREPOINT_CLIENT_ID,
                    self.SHAREPOINT_THUMBPRINT,
                    private_key=private_key_content
                )
                return self._ctx
            except Exception as e:
                logger.error(f"Certificate authentication failed: {e}")
                raise ValueError(f"Certificate authentication failed: {e}")

        # Fallback: Try with certificate paths (alternative method)
        elif all([self.SHAREPOINT_CLIENT_ID, self.SHAREPOINT_CERTIFICATE_PATH, self.SHAREPOINT_PRIVATE_KEY_PATH]):
            logger.info("Using certificate authentication with file paths for SharePoint")
            try:
                # Calculate thumbprint from certificate if not provided
                if not self.SHAREPOINT_THUMBPRINT:
                    import subprocess
                    result = subprocess.run([
                        'openssl', 'x509', '-noout', '-fingerprint', '-sha1',
                        '-in', self.SHAREPOINT_CERTIFICATE_PATH
                    ], capture_output=True, text=True)
                    if result.returncode == 0:
                        thumbprint = result.stdout.split('=')[1].strip().replace(':', '')
                    else:
                        raise ValueError("Could not calculate certificate thumbprint")
                else:
                    thumbprint = self.SHAREPOINT_THUMBPRINT

                # Read the private key content
                with open(self.SHAREPOINT_PRIVATE_KEY_PATH, 'r') as f:
                    private_key_content = f.read()

                self._ctx = ClientContext(self.SHAREPOINT_URL).with_client_certificate(
                    self.SHAREPOINT_CLIENT_ID,
                    thumbprint,
                    private_key_content
                )
                return self._ctx
            except Exception as e:
                logger.error(f"Certificate authentication failed: {e}")
                raise ValueError(f"Certificate authentication failed: {e}")

        # Fallback to username/password authentication (deprecated)
        elif all([self.SHAREPOINT_USER, self.SHAREPOINT_PASSWORD]):
            logger.warning("Using deprecated username/password authentication for SharePoint. Consider switching to certificate authentication.")
            self._ctx = ClientContext(self.SHAREPOINT_URL).with_credentials(
                UserCredential(self.SHAREPOINT_USER, self.SHAREPOINT_PASSWORD)
            )
            return self._ctx

        else:
            raise ValueError(
                "SharePoint authentication failed. Please provide either:\n"
                "1. Certificate authentication: SHAREPOINT_CLIENT_ID, SHAREPOINT_TENANT_ID, "
                "SHAREPOINT_CERTIFICATE_PATH, SHAREPOINT_PRIVATE_KEY_PATH\n"
                "2. Username/password authentication (deprecated): SHAREPOINT_USER, SHAREPOINT_PASSWORD"
            )

    def latest_sharepoint_dataset(self, folder_server_relative_url="/sites/YourSite/Shared%20Documents/YourFolder", filename=None):
        """Let
        Gets the most recently modified file from a SharePoint directory as a download stream.

        This method connects to a SharePoint site, finds the most recent file
        in a specified folder, and returns a download session stream.

        Args:
            folder_server_relative_url: The SharePoint folder path
            filename: Optional specific filename (currently unused)

        Returns:
            Tuple[stream, str]: A tuple containing the download stream and filename
        """

        # Authenticate and create a client context
        ctx = self.authenticate()

        # Get the folder from SharePoint
        folder = ctx.web.get_folder_by_server_relative_url(folder_server_relative_url)
        files = folder.files.get().execute_query()

        if not files:
            raise FileNotFoundError(f"No files found in SharePoint folder: {folder_server_relative_url}")

        # Find the most recently modified file
        latest_file = sorted(files, key=lambda f: f.properties["TimeLastModified"], reverse=True)[0]
        latest_file_name = latest_file.properties["Name"]
        print(f"Found latest file: {latest_file_name}")

        # Download file content to BytesIO stream
        file_stream = io.BytesIO()
        latest_file.download(file_stream).execute_query()

        # Reset stream position to beginning for reading
        file_stream.seek(0)

        print(f"File '{latest_file_name}' downloaded to stream (size: {file_stream.getbuffer().nbytes} bytes)")

        return file_stream, latest_file_name




    def updated_sharepoint_files(self, folder_server_relative_url="/sites/YourSite/Shared%20Documents/YourFolder"):
        """
        Downloads files from a SharePoint directory that have been updated
        since a specific date.
        """


        ctx = self.authenticate()

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
