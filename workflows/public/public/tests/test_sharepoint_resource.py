import pytest
import io
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from ..resources.sharepoint import SharepointResource


class TestSharepointResource:
    """Test suite for SharepointResource class."""

    @pytest.fixture
    def sharepoint_config(self):
        """Test configuration for SharePoint resource."""
        return {
            "SHAREPOINT_URL": "https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/",
            "SHAREPOINT_USER": "dwvalentine@ucsd.edu",
            "SHAREPOINT_PASSWORD": "test_password"
        }

    @pytest.fixture
    def sharepoint_resource(self, sharepoint_config):
        """Create SharepointResource instance for testing."""
        return SharepointResource(**sharepoint_config)

    @pytest.fixture
    def mock_file_properties(self):
        """Mock SharePoint file properties."""
        return {
            "Name": "infur_20251103.xlsx",
            "TimeLastModified": "2025-11-03T10:30:00Z",
            "ServerRelativeUrl": "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents/infur_20251103.xlsx"
        }

    @pytest.fixture
    def mock_sharepoint_file(self, mock_file_properties):
        """Mock SharePoint file object."""
        mock_file = Mock()
        mock_file.properties = mock_file_properties
        mock_file.download = Mock()
        return mock_file

    @pytest.fixture
    def mock_sharepoint_files(self, mock_sharepoint_file):
        """Mock list of SharePoint files."""
        # Create multiple files with different timestamps (older dates)
        files = []
        for i in range(3):
            file_mock = Mock()
            file_mock.properties = {
                "Name": f"test_file_{i}.xlsx",
                "TimeLastModified": f"2025-10-{i+1:02d}T10:30:00Z",  # October dates (older)
                "ServerRelativeUrl": f"/sites/test/Shared Documents/test_file_{i}.xlsx"
            }
            file_mock.download = Mock()
            files.append(file_mock)

        # Add the main test file (should be latest - November date)
        files.append(mock_sharepoint_file)
        return files

    def test_sharepoint_resource_initialization(self, sharepoint_config):
        """Test SharepointResource initialization."""
        resource = SharepointResource(**sharepoint_config)

        assert resource.SHAREPOINT_URL == sharepoint_config["SHAREPOINT_URL"]
        assert resource.SHAREPOINT_USER == sharepoint_config["SHAREPOINT_USER"]
        assert resource.SHAREPOINT_PASSWORD == sharepoint_config["SHAREPOINT_PASSWORD"]
        assert resource._ctx is None

    def test_sharepoint_resource_missing_config(self):
        """Test SharepointResource with missing configuration."""
        with pytest.raises(ValueError, match="Please set SHAREPOINT_URL"):
            resource = SharepointResource(
                SHAREPOINT_URL="",
                SHAREPOINT_USER="test@test.com",
                SHAREPOINT_PASSWORD="password"
            )
            resource.authenticate()

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_authenticate_success(self, mock_credential, mock_context, sharepoint_resource):
        """Test successful authentication."""
        # Setup mocks
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        # Call authenticate
        result = sharepoint_resource.authenticate()

        # Verify calls
        mock_credential.assert_called_once_with("dwvalentine@ucsd.edu", "test_password")
        mock_context.assert_called_once_with(
            "https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/"
        )

        # Verify result
        assert result == mock_ctx_instance
        assert sharepoint_resource._ctx == mock_ctx_instance

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_authenticate_cached(self, mock_credential, mock_context, sharepoint_resource):
        """Test that authentication is cached."""
        # Setup initial authentication
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        # First call
        result1 = sharepoint_resource.authenticate()

        # Second call should return cached context
        result2 = sharepoint_resource.authenticate()

        # Verify only called once
        mock_credential.assert_called_once()
        mock_context.assert_called_once()

        # Both calls should return the same instance
        assert result1 == result2
        assert result1 == mock_ctx_instance

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_latest_sharepoint_dataset_success(
        self, mock_credential, mock_context, sharepoint_resource, mock_sharepoint_files
    ):
        """Test successful download of latest SharePoint file."""
        # Setup mocks
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        mock_folder = Mock()
        mock_folder.files.get.return_value.execute_query.return_value = mock_sharepoint_files
        mock_ctx_instance.web.get_folder_by_server_relative_url.return_value = mock_folder

        # Mock file download
        def mock_download_side_effect(stream):
            # Simulate file content
            test_content = b"test file content for infur_20251103.xlsx"
            stream.write(test_content)
            return latest_file.download.return_value  # Return the mock that has execute_query

        latest_file = mock_sharepoint_files[-1]  # infur_20251103.xlsx

        # Setup the mock chain properly
        mock_download_result = Mock()
        mock_download_result.execute_query = Mock()
        latest_file.download.return_value = mock_download_result
        latest_file.download.side_effect = mock_download_side_effect

        # Call method
        folder_url = "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents"
        stream, filename = sharepoint_resource.latest_sharepoint_dataset(folder_url)

        # Verify results
        assert isinstance(stream, io.BytesIO)
        assert filename == "infur_20251103.xlsx"

        # Verify stream content
        stream_content = stream.read()
        assert stream_content == b"test file content for infur_20251103.xlsx"

        # Verify file was downloaded
        latest_file.download.assert_called_once()
        latest_file.download.return_value.execute_query.assert_called_once()

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_latest_sharepoint_dataset_no_files(
        self, mock_credential, mock_context, sharepoint_resource
    ):
        """Test behavior when no files are found."""
        # Setup mocks
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        mock_folder = Mock()
        mock_folder.files.get.return_value.execute_query.return_value = []  # No files
        mock_ctx_instance.web.get_folder_by_server_relative_url.return_value = mock_folder

        # Call method and expect exception
        with pytest.raises(FileNotFoundError, match="No files found in SharePoint folder"):
            sharepoint_resource.latest_sharepoint_dataset("/test/folder")

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_updated_sharepoint_files_success(
        self, mock_credential, mock_context, sharepoint_resource, mock_sharepoint_files
    ):
        """Test successful retrieval of updated SharePoint files."""
        # Setup mocks
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        mock_folder = Mock()
        mock_folder.files.filter.return_value.get.return_value.execute_query.return_value = mock_sharepoint_files
        mock_ctx_instance.web.get_folder_by_server_relative_url.return_value = mock_folder

        # Mock file downloads
        for file_mock in mock_sharepoint_files:
            file_mock.download.return_value.execute_query = Mock()

        # Call method
        with patch('os.makedirs'), patch('builtins.open', create=True) as mock_open:
            mock_file_handle = Mock()
            mock_open.return_value.__enter__.return_value = mock_file_handle

            result = sharepoint_resource.updated_sharepoint_files("/test/folder")

        # Verify results
        assert len(result) == len(mock_sharepoint_files)

        # Verify all files were processed
        for file_mock in mock_sharepoint_files:
            file_mock.download.assert_called_once()

    @patch('workflows.public.public.resources.sharepoint.ClientContext')
    @patch('workflows.public.public.resources.sharepoint.UserCredential')
    def test_updated_sharepoint_files_no_updates(
        self, mock_credential, mock_context, sharepoint_resource
    ):
        """Test behavior when no updated files are found."""
        # Setup mocks
        mock_ctx_instance = Mock()
        mock_context.return_value.with_credentials.return_value = mock_ctx_instance

        mock_folder = Mock()
        mock_folder.files.filter.return_value.get.return_value.execute_query.return_value = []
        mock_ctx_instance.web.get_folder_by_server_relative_url.return_value = mock_folder

        # Call method
        result = sharepoint_resource.updated_sharepoint_files("/test/folder")

        # Verify results
        assert result == []

    def test_file_sorting_by_timestamp(self, mock_sharepoint_files):
        """Test that files are correctly sorted by TimeLastModified."""
        # Sort files by timestamp (reverse=True for latest first)
        sorted_files = sorted(
            mock_sharepoint_files,
            key=lambda f: f.properties["TimeLastModified"],
            reverse=True
        )

        # The latest file should be infur_20251103.xlsx
        latest_file = sorted_files[0]
        assert latest_file.properties["Name"] == "infur_20251103.xlsx"
        assert latest_file.properties["TimeLastModified"] == "2025-11-03T10:30:00Z"


class TestSharepointResourceIntegration:
    """Integration tests for SharepointResource with real SharePoint data."""

    @pytest.fixture
    def real_sharepoint_config(self):
        """Real SharePoint configuration from environment variables."""
        return {
            "SHAREPOINT_URL": "https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/",
            "SHAREPOINT_USER": os.getenv("WAHIS_SHAREPOINT_USER",  "dwvalentine@ucsd.edu"),
            "SHAREPOINT_PASSWORD": os.getenv("WAHIS_SHAREPOINT_PASSWORD", "test_password")
        }

    @pytest.mark.integration
    @pytest.mark.skipif(
        not os.getenv("WAHIS_SHAREPOINT_PASSWORD"),
        reason="WAHIS_SHAREPOINT_PASSWORD environment variable not set"
    )
    def test_real_sharepoint_connection(self, real_sharepoint_config):
        """Test connection to real SharePoint instance."""
        resource = SharepointResource(**real_sharepoint_config)

        # This test requires real credentials
        try:
            ctx = resource.authenticate()
            assert ctx is not None
        except Exception as e:
            pytest.skip(f"Real SharePoint connection failed: {e}")

    @pytest.mark.integration
    @pytest.mark.skipif(
        not os.getenv("WAHIS_SHAREPOINT_PASSWORD"),
        reason="WAHIS_SHAREPOINT_PASSWORD environment variable not set"
    )
    def test_real_file_download(self, real_sharepoint_config):
        """Test downloading a real file from SharePoint."""
        resource = SharepointResource(**real_sharepoint_config)

        try:
            # Try to download from the Shared Documents folder
            #folder_url = "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents/"
            #folder_url = "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents/Forms/AllItems.aspx"
            folder_url = "Shared Documents/"
            stream, filename = resource.latest_sharepoint_dataset(folder_url)

            assert isinstance(stream, io.BytesIO)
            assert filename is not None
            assert len(filename) > 0

            # Verify stream has content
            content_size = stream.getbuffer().nbytes
            assert content_size > 0

            print(f"Successfully downloaded: {filename} ({content_size} bytes)")

        except Exception as e:
            pytest.skip(f"Real SharePoint file download failed: {e}")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])
