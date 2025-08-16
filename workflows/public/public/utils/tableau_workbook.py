import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import requests
from dagster import Config, get_dagster_logger
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


class TableauWorkbookConfig(Config):
    url: str = "https://public.tableau.com/workbooks/DraftRespDash.twb"
    workbook_name: str = "sandiego_epidemiology"



class TableauWorkbookProcessor:
    """Utility class for processing Tableau workbooks"""

    def __init__(self, logger=None):
        self.logger = logger or get_dagster_logger()

    def download_workbook(self, url: str) -> bytes:
        """Download workbook from URL"""
        self.logger.info(f"Downloading workbook from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        return response.content

    def extract_workbook(self, workbook_content: bytes, extract_dir: Path) -> Dict[str, Any]:
        """Extract workbook zip file and return metadata"""
        extract_dir.mkdir(parents=True, exist_ok=True)

        # Save workbook content to temp file
        with tempfile.NamedTemporaryFile(suffix='.twb', delete=False) as temp_file:
            temp_file.write(workbook_content)
            temp_path = temp_file.name

        try:
            # Extract the workbook
            with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                file_list = zip_ref.namelist()

            self.logger.info(f"Extracted {len(file_list)} files")

            # Find hyper files
            hyper_files = list(extract_dir.rglob("*.hyper"))

            return {
                "extracted_files": file_list,
                "hyper_files": [str(f.relative_to(extract_dir)) for f in hyper_files],
                "hyper_count": len(hyper_files)
            }

        finally:
            os.unlink(temp_path)

    def extract_hyper_data(self, hyper_file_path: Path) -> Dict[str, pd.DataFrame]:
        """Extract data from Hyper file and return DataFrames"""
        if not hyper_file_path.exists():
            self.logger.error(f"Hyper file not found: {hyper_file_path}")
            return {}

        if hyper_file_path.stat().st_size == 0:
            self.logger.error(f"Hyper file is empty: {hyper_file_path}")
            return {}

        extracted_data = {}

        try:
            hyper_path = str(hyper_file_path.absolute())
            self.logger.info(f"Processing Hyper file: {hyper_file_path.name}")

            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.NONE) as connection:
                    # Get all table names
                    tables = connection.catalog.get_table_names("Extract")
                    self.logger.info(f"Found {len(tables)} tables")

                    for table in tables:
                        try:
                            # Get table definition
                            table_def = connection.catalog.get_table_definition(table)
                            columns = [col.name for col in table_def.columns]

                            # Query all data
                            query = f"SELECT * FROM {table}"
                            with connection.execute_query(query) as result:
                                rows = list(result)

                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                # Clean table name for key
                                clean_name = str(table.name).replace('"', '').replace('.', '_').replace(' ', '_')
                                #table_key = f"{hyper_file_path.stem}_{clean_name}"
                                table_key = f"{clean_name}"
                                df = df.rename(columns=lambda x: f"{x}".replace('"',''))
                                extracted_data[table_key] = df

                                self.logger.info(f"Extracted table {table.name}: {len(df)} rows, {len(df.columns)} columns")

                        except Exception as table_error:
                            self.logger.error(f"Error processing table {table}: {table_error}")
                            continue

        except Exception as e:
            self.logger.error(f"Error processing Hyper file: {e}")

            # Try fallback with simplified filename
            try:
                simple_name = hyper_file_path.parent / f"temp_{hyper_file_path.stem.replace(' ', '_')}.hyper"
                shutil.copy2(hyper_file_path, simple_name)

                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database=str(simple_name.absolute())) as connection:
                        tables = connection.catalog.get_table_names("Extract")

                        for table in tables:
                            table_def = connection.catalog.get_table_definition(table)
                            columns = [col.name for col in table_def.columns]

                            query = f"SELECT * FROM {table}"
                            with connection.execute_query(query) as result:
                                rows = list(result)

                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                clean_name = str(table.name).replace('"', '').replace('.', '_').replace(' ', '_')
                                #table_key = f"{hyper_file_path.stem}_{clean_name}"
                                table_key = f"{clean_name}"
                                df = df.rename(columns=lambda x: f"{x}".replace('"', ''))
                                extracted_data[table_key] = df

                simple_name.unlink()  # Clean up
                self.logger.info("Fallback extraction successful")

            except Exception as fallback_error:
                self.logger.error(f"Fallback extraction also failed: {fallback_error}")

        return extracted_data
