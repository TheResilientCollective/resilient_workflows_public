"""
Tableau data extraction utilities for San Diego epidemiology data.

This module provides functions to extract data from Tableau Public workbooks,
specifically targeting the San Diego County epidemiology dashboard.
"""

import requests
import pandas as pd
import os
from pathlib import Path
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName
import zipfile
import json
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional, Tuple
from dagster import get_dagster_logger


class TableauExtractor:
    """
    Extracts data from Tableau Public workbooks.
    """

    def __init__(self, base_url: str = "https://public.tableau.com"):
        self.base_url = base_url
        self.logger = get_dagster_logger()

    def download_direct_workbook(self, workbook_url: str, output_dir: str) -> Optional[Path]:
        """
        Download workbook directly from the provided URL.

        Args:
            workbook_url: Direct URL to the workbook file
            output_dir: Directory to save the file

        Returns:
            Path to downloaded file, or None if failed
        """
        try:
            self.logger.info(f"Downloading workbook from: {workbook_url}")

            # Try different file extensions and URL variations
            urls_to_try = [
                workbook_url,
                workbook_url + 'x' if workbook_url.endswith('.twb') else workbook_url,
                workbook_url.replace('.twb', '.twbx') if '.twb' in workbook_url else workbook_url + '.twbx'
            ]

            for url in urls_to_try:
                try:
                    self.logger.info(f"Trying URL: {url}")
                    response = requests.get(url, timeout=30)

                    if response.status_code == 200 and len(response.content) > 1000:
                        # Determine filename from URL
                        filename = Path(url).name
                        if not filename.endswith(('.twb', '.twbx')):
                            filename = filename + '.twbx'

                        file_path = Path(output_dir) / filename
                        with open(file_path, 'wb') as f:
                            f.write(response.content)

                        self.logger.info(f"Downloaded: {filename} ({len(response.content)} bytes)")

                        # Validate file type
                        if filename.endswith('.twbx') and response.content.startswith(b'PK'):
                            self.logger.info("File appears to be a valid ZIP/TWBX archive")
                        elif filename.endswith('.twb'):
                            self.logger.info("File appears to be a TWB workbook")

                        return file_path

                except Exception as e:
                    self.logger.debug(f"Failed to download from {url}: {e}")
                    continue

            self.logger.warning("Could not download workbook from any URL")
            return None

        except Exception as e:
            self.logger.error(f"Error in direct workbook download: {e}")
            return None

    def extract_twbx_data(self, twbx_file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Extract data from a TWBX file (ZIP archive).

        Args:
            twbx_file_path: Path to the TWBX file

        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        extracted_data = {}

        try:
            import zipfile

            with zipfile.ZipFile(twbx_file_path, 'r') as zip_file:
                self.logger.info(f"Extracting contents from {twbx_file_path}")

                # Create temporary extraction directory
                import tempfile
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip_file.extractall(temp_dir)

                    # Look for data files
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            file_path = Path(root) / file

                            try:
                                if file.endswith('.hyper'):
                                    # Extract hyper data
                                    hyper_data = self.extract_hyper_data(str(file_path))
                                    if hyper_data:
                                        extracted_data.update(hyper_data)

                                elif file.endswith('.csv'):
                                    df = pd.read_csv(file_path)
                                    extracted_data[file_path.stem] = df
                                    self.logger.info(f"Extracted CSV: {file_path.stem} ({df.shape})")

                                elif file.endswith('.json'):
                                    with open(file_path, 'r') as f:
                                        json_data = json.load(f)
                                    # Convert JSON to DataFrame if possible
                                    if isinstance(json_data, list):
                                        df = pd.DataFrame(json_data)
                                        extracted_data[file_path.stem] = df
                                        self.logger.info(f"Extracted JSON: {file_path.stem} ({df.shape})")

                            except Exception as e:
                                self.logger.warning(f"Could not extract data from {file}: {e}")
                                continue

            return extracted_data

        except Exception as e:
            self.logger.error(f"Error extracting TWBX file: {e}")
            return {}

    def extract_tableau_session_data(self, dashboard_url: str, document_id: str = "{6F324CCD-B1F2-4F80-AA86-8AD270C97348}") -> Dict[str, pd.DataFrame]:
        """
        Extract data from Tableau dashboard using session-based API calls.

        This method replicates the JavaScript approach:
        1. Get session key from dashboard page
        2. Use session to get export key via API
        3. Download CSV data using the export key

        Args:
            dashboard_url: URL to the Tableau dashboard
            document_id: Document ID for the specific data sheet

        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        try:
            session = requests.Session()
            extracted_data = {}

            # Step 1: Visit dashboard page to establish session
            self.logger.info(f"Visiting dashboard to establish session: {dashboard_url}")
            response = session.get(dashboard_url)
            response.raise_for_status()

            # Extract session key from the page
            session_key = self._extract_session_key(response.text)
            if not session_key:
                self.logger.error("Could not extract session key from dashboard")
                return {}

            self.logger.info(f"Found session key: {session_key[:10]}...")

            # Step 2: Get export key using the session
            export_key = self._get_export_key(session, session_key, document_id)

            if not export_key:
                self.logger.error("Could not get export key")
                return {}

            self.logger.info(f"Got export key: {export_key[:10]}...")

            # Step 3: Download CSV data using the export key
            csv_data = self._download_csv_data(session, session_key, export_key)

            if csv_data:
                # Parse CSV data into DataFrame
                from io import StringIO
                df = pd.read_csv(StringIO(csv_data))
                extracted_data['tableau_session_data'] = df
                self.logger.info(f"Successfully extracted CSV data: {df.shape}")

            return extracted_data

        except Exception as e:
            self.logger.error(f"Error in session-based extraction: {e}")
            return {}

    def _extract_session_key(self, html_content: str) -> Optional[str]:
        """Extract session key from dashboard HTML."""
        try:
            # Look for session key patterns in the HTML
            patterns = [
                r'"sessionid":"([^"]+)"',
                r'"session":"([^"]+)"',
                r'sessionId["\']:\s*["\']([^"\']+)["\']',
                r'session["\']:\s*["\']([^"\']+)["\']'
            ]

            for pattern in patterns:
                match = re.search(pattern, html_content)
                if match:
                    return match.group(1)

            # Try to find it in script tags
            soup = BeautifulSoup(html_content, 'html.parser')
            scripts = soup.find_all('script')

            for script in scripts:
                if script.string:
                    for pattern in patterns:
                        match = re.search(pattern, script.string)
                        if match:
                            return match.group(1)

            return None

        except Exception as e:
            self.logger.error(f"Error extracting session key: {e}")
            return None

    def _get_export_key(self, session: requests.Session, session_key: str, document_id: str) -> Optional[str]:
        """Get export key using the session."""
        try:
            # Replicate the JavaScript fetch call
            url = f"https://public.tableau.com/vizql/w/DraftRespDash/v/RespDash/sessions/{session_key}/commands/tabsrv/export-crosstab-to-csvserver"

            # Create multipart form data body
            boundary = "TLArn3Eh"
            body_parts = [
                f'--{boundary}\r\nContent-Disposition: form-data; name="sheetdocId"\r\n\r\n{document_id}\r\n',
                f'--{boundary}\r\nContent-Disposition: form-data; name="useTabs"\r\n\r\ntrue\r\n',
                f'--{boundary}\r\nContent-Disposition: form-data; name="sendNotifications"\r\n\r\ntrue\r\n',
                f'--{boundary}\r\nContent-Disposition: form-data; name="telemetryCommandId"\r\n\r\n1j18vivat$6u0j-8k-ge-rg-t656ko\r\n',
                f'--{boundary}--\r\n'
            ]
            body = ''.join(body_parts)

            headers = {
                "accept": "text/javascript",
                "accept-language": "en-US,en;q=0.7",
                "content-type": f"multipart/form-data; boundary={boundary}",
                "priority": "u=1, i",
                "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "sec-gpc": "1",
                "x-b3-sampled": "1",
                "x-requested-with": "XMLHttpRequest",
                "x-tableau-version": "2025.2",
                "x-tsi-active-tab": "RespDash"
            }

            response = session.post(url, data=body, headers=headers)
            response.raise_for_status()

            # Parse the JSON response to get the export key
            response_data = response.json()

            # Navigate the JSON structure as shown in JavaScript
            try:
                export_key = response_data['vqlCmdResponse']['layoutStatus']['applicationPresModel']['presentationLayerNotification'][0]['presModelHolder']['genExportFilePresModel']['resultKey']
                return export_key
            except KeyError as e:
                self.logger.error(f"Could not find export key in response structure: {e}")
                self.logger.debug(f"Response structure: {response_data}")
                return None

        except Exception as e:
            self.logger.error(f"Error getting export key: {e}")
            return None

    def _download_csv_data(self, session: requests.Session, session_key: str, export_key: str) -> Optional[str]:
        """Download CSV data using the export key."""
        try:
            url = f"https://public.tableau.com/vizql/w/DraftRespDash/v/RespDash/tempfile/sessions/{session_key}/?key={export_key}&keepfile=yes&attachment=yes"

            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.7",
                "priority": "u=1, i",
                "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "sec-gpc": "1",
                "x-b3-sampled": "1",
                "x-requested-with": "XMLHttpRequest",
                "x-tableau-version": "2025.2",
                "x-tsi-active-tab": "RespDash"
            }

            response = session.get(url, headers=headers)
            response.raise_for_status()

            return response.text

        except Exception as e:
            self.logger.error(f"Error downloading CSV data: {e}")
            return None

    def get_profile_workbooks(self, profile_url: str) -> List[str]:
        """
        Extract workbook URLs from a Tableau Public profile page.

        Args:
            profile_url: URL to the Tableau Public profile

        Returns:
            List of workbook URLs found on the profile
        """
        try:
            response = requests.get(profile_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for workbook links - these typically have patterns like:
            # /views/WorkbookName/Sheet1
            workbook_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/views/' in href and '/sheet' not in href.lower():
                    workbook_links.append(href)

            # Remove duplicates and clean up
            workbook_links = list(set(workbook_links))
            self.logger.info(f"Found {len(workbook_links)} workbook links")

            return workbook_links

        except Exception as e:
            self.logger.error(f"Error fetching profile workbooks: {e}")
            return []

    def download_workbook_data(self, workbook_url: str, format_type: str = 'csv') -> Optional[bytes]:
        """
        Attempt to download workbook data in specified format.

        Args:
            workbook_url: URL to the workbook
            format_type: Format to download (csv, xlsx, pdf, png, hyper)

        Returns:
            Raw bytes of the downloaded file, or None if failed
        """
        if not workbook_url.startswith('http'):
            workbook_url = self.base_url + workbook_url

        # Try different download URL patterns
        download_patterns = [
            f"{workbook_url}.{format_type}",
            f"{workbook_url}/download.{format_type}",
            f"{workbook_url}?format={format_type}",
            workbook_url.replace('/views/', '/t/') + f'/download.{format_type}'
        ]

        for pattern in download_patterns:
            try:
                self.logger.info(f"Trying download pattern: {pattern}")
                response = requests.get(pattern, timeout=30)
                if response.status_code == 200:
                    self.logger.info(f"Successfully downloaded {len(response.content)} bytes")
                    return response.content
            except Exception as e:
                self.logger.debug(f"Download pattern failed: {e}")
                continue

        self.logger.warning(f"Could not download workbook data in format {format_type}")
        return None

    def extract_hyper_data(self, hyper_path: str) -> Dict[str, pd.DataFrame]:
        """
        Extract data from a Tableau .hyper file.

        Args:
            hyper_path: Path to the .hyper file

        Returns:
            Dictionary mapping table names to DataFrames
        """
        if not os.path.exists(hyper_path):
            self.logger.error(f"Hyper file not found: {hyper_path}")
            return {}

        extracted_data = {}

        try:
            # Start Hyper API process
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(endpoint=hyper.endpoint, database=hyper_path) as connection:
                    # List all schemas
                    schemas = connection.catalog.get_schema_names()
                    self.logger.info(f"Available schemas: {schemas}")

                    for schema in schemas:
                        # List all tables in this schema
                        tables = connection.catalog.get_table_names(schema)
                        self.logger.info(f"Tables in schema '{schema}': {[t.name for t in tables]}")

                        for table in tables:
                            table_name = TableName(schema, table.name)
                            self.logger.info(f"Reading from table: {table_name}")

                            try:
                                # Get table definition
                                table_def = connection.catalog.get_table_definition(table_name)
                                columns = [col.name for col in table_def.columns]

                                # Query all rows
                                query = f"SELECT * FROM {table_name}"
                                result = connection.execute_list_query(query)

                                # Convert to pandas DataFrame
                                df = pd.DataFrame(result, columns=columns)
                                self.logger.info(f"Extracted {len(df)} rows from {table_name}")

                                # Store in our extracted data
                                key = f"{schema}_{table.name}"
                                extracted_data[key] = df

                            except Exception as e:
                                self.logger.error(f"Error extracting table {table_name}: {e}")
                                continue

        except Exception as e:
            self.logger.error(f"Error extracting hyper data: {e}")
            return {}

        return extracted_data

    def extract_data_from_webpage(self, workbook_url: str) -> Dict[str, any]:
        """
        Attempt to extract data directly from the Tableau webpage.

        Args:
            workbook_url: URL to the workbook

        Returns:
            Dictionary containing extracted data and metadata
        """
        if not workbook_url.startswith('http'):
            workbook_url = self.base_url + workbook_url

        try:
            response = requests.get(workbook_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            extracted = {
                'html_content': response.text,
                'json_objects': [],
                'csv_data': [],
                'metadata': {}
            }

            # Look for data in script tags or embedded JSON
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and ('data' in script.string.lower() or 'json' in script.string.lower()):
                    # Try to extract JSON data
                    script_content = script.string
                    # Look for JSON-like patterns
                    json_matches = re.findall(r'\\{[^{}]*"[^"]*"[^{}]*\\}', script_content)
                    extracted['json_objects'].extend(json_matches)

            # Look for CSV data in the page
            data_elements = soup.find_all(attrs={"data-csv": True})
            for elem in data_elements:
                csv_data = elem.get('data-csv')
                if csv_data:
                    extracted['csv_data'].append(csv_data)

            # Extract metadata
            title_elem = soup.find('title')
            if title_elem:
                extracted['metadata']['title'] = title_elem.get_text()

            meta_description = soup.find('meta', attrs={'name': 'description'})
            if meta_description:
                extracted['metadata']['description'] = meta_description.get('content')

            self.logger.info(f"Extracted {len(extracted['json_objects'])} JSON objects and {len(extracted['csv_data'])} CSV datasets")

            return extracted

        except Exception as e:
            self.logger.error(f"Error extracting data from webpage: {e}")
            return {}

    def save_extracted_data(self, data: Dict[str, pd.DataFrame], output_dir: Path) -> List[Path]:
        """
        Save extracted DataFrames to CSV files.

        Args:
            data: Dictionary mapping names to DataFrames
            output_dir: Directory to save files to

        Returns:
            List of file paths that were created
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        saved_files = []

        for name, df in data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Clean up the name for filename
                clean_name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
                csv_path = output_dir / f"{clean_name}.csv"

                try:
                    df.to_csv(csv_path, index=False)
                    saved_files.append(csv_path)
                    self.logger.info(f"Saved {len(df)} rows to {csv_path}")
                except Exception as e:
                    self.logger.error(f"Error saving {name} to CSV: {e}")

        return saved_files


