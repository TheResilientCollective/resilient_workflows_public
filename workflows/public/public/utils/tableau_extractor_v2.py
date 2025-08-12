import requests
import zipfile
import os
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from tableauhyperapi import HyperProcess, Endpoint, Connection, TableName
import tempfile
import shutil


class TableauWorkbookAnalyzer:
    def __init__(self, url, download_dir="tableau_analysis"):
        self.url = url
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.workbook_path = None
        self.data_dir = None

    def download_and_extract(self):
        """Download the zip file and extract it"""
        print(f"Downloading from: {self.url}")

        # Download the zip file
        response = requests.get(self.url, stream=True)
        response.raise_for_status()

        zip_path = self.download_dir / "workbook.zip"
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Downloaded to: {zip_path}")

        # Extract the zip file
        extract_dir = self.download_dir / "extracted"
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        print(f"Extracted to: {extract_dir}")

        # Find the .twb file and data directory
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.twb'):
                    self.workbook_path = Path(root) / file
                    print(f"Found workbook: {self.workbook_path}")

            if 'data' in dirs:
                self.data_dir = Path(root) / 'data'
                print(f"Found data directory: {self.data_dir}")

        return self.workbook_path, self.data_dir

    def analyze_workbook_xml(self):
        """Analyze the .twb file to find data sources"""
        if not self.workbook_path or not self.workbook_path.exists():
            raise FileNotFoundError("Workbook file not found")

        print(f"\n=== Analyzing workbook XML: {self.workbook_path.name} ===")

        # Parse the XML
        tree = ET.parse(self.workbook_path)
        root = tree.getroot()

        datasources = []

        # Find all datasource elements
        for datasource in root.findall('.//datasource'):
            ds_info = {
                'name': datasource.get('name', 'Unknown'),
                'caption': datasource.get('caption', ''),
                'connections': [],
                'relations': []
            }

            # Find connections
            for connection in datasource.findall('.//connection'):
                conn_info = {
                    'class': connection.get('class', ''),
                    'server': connection.get('server', ''),
                    'dbname': connection.get('dbname', ''),
                    'filename': connection.get('filename', ''),
                    'directory': connection.get('directory', ''),
                    'schema': connection.get('schema', ''),
                }
                ds_info['connections'].append(conn_info)

            # Find relations (tables, joins, etc.)
            for relation in datasource.findall('.//relation'):
                rel_info = {
                    'name': relation.get('name', ''),
                    'table': relation.get('table', ''),
                    'type': relation.get('type', ''),
                }
                ds_info['relations'].append(rel_info)

            datasources.append(ds_info)

        # Print datasource information
        for i, ds in enumerate(datasources, 1):
            print(f"\nDatasource {i}: {ds['name']}")
            if ds['caption']:
                print(f"  Caption: {ds['caption']}")

            for j, conn in enumerate(ds['connections'], 1):
                print(f"  Connection {j}:")
                for key, value in conn.items():
                    if value:
                        print(f"    {key}: {value}")

            for j, rel in enumerate(ds['relations'], 1):
                print(f"  Relation {j}:")
                for key, value in rel.items():
                    if value:
                        print(f"    {key}: {value}")

        return datasources

    def list_data_files(self):
        """List all files in the data directory"""
        if not self.data_dir or not self.data_dir.exists():
            print("No data directory found")
            return []

        print(f"\n=== Files in data directory ===")
        data_files = []

        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                file_path = Path(root) / file
                file_size = file_path.stat().st_size
                data_files.append({
                    'path': file_path,
                    'name': file,
                    'size': file_size,
                    'extension': file_path.suffix
                })
                print(f"  {file} ({file_size:,} bytes)")

        return data_files

    def read_hyper_files(self, export_csv=False):
        """Read Tableau Hyper files (.hyper) if any exist"""
        if not self.data_dir:
            return []

        hyper_files = list(self.data_dir.rglob("*.hyper"))

        if not hyper_files:
            print("No .hyper files found")
            return []

        print(f"\n=== Reading Hyper files ===")
        hyper_data = []

        # Create CSV export directory if needed
        if export_csv:
            csv_dir = self.download_dir / "csv_exports"
            csv_dir.mkdir(parents=True, exist_ok=True)
            print(f"CSV exports will be saved to: {csv_dir.absolute()}")

            # Test write permissions
            test_file = csv_dir / "test_write.txt"
            try:
                test_file.write_text("test")
                test_file.unlink()
                print(f"✅ Write permissions confirmed")
            except Exception as e:
                print(f"❌ Write permission test failed: {e}")
                return []

        for hyper_file in hyper_files:
            print(f"\nProcessing: {hyper_file.name}")

            try:
                with HyperProcess(telemetry=Endpoint.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database=str(hyper_file)) as connection:
                        # Get all table names
                        table_names = connection.catalog.get_table_names("Extract")

                        for table_name in table_names:
                            print(f"  Table: {table_name}")

                            # Get table definition
                            table_def = connection.catalog.get_table_definition(table_name)
                            print(f"    Columns: {len(table_def.columns)}")

                            for column in table_def.columns:
                                print(f"      - {column.name} ({column.type})")

                            # Read ALL data for export, or sample for analysis
                            if export_csv:
                                query = f'SELECT * FROM {table_name}'
                                print(f"    Reading full table for CSV export...")
                            else:
                                query = f'SELECT * FROM {table_name} LIMIT 100'
                                print(f"    Reading sample data...")

                            with connection.execute_query(query) as result:
                                rows = list(result)
                                print(f"    Rows read: {len(rows)}")

                                if rows:
                                    # Convert to DataFrame
                                    columns = [col.name for col in table_def.columns]
                                    df = pd.DataFrame(rows, columns=columns)

                                    hyper_data.append({
                                        'file': hyper_file.name,
                                        'table': str(table_name),
                                        'dataframe': df,
                                        'schema': table_def
                                    })

                                    print(f"    DataFrame shape: {df.shape}")

                                    # Export to CSV if requested
                                    if export_csv:
                                        # Clean table name for filename
                                        clean_table_name = str(table_name).replace('"', '').replace('.', '_').replace(
                                            ' ', '_')
                                        csv_filename = f"{hyper_file.stem}_{clean_table_name}.csv"
                                        csv_path = csv_dir / csv_filename

                                        try:
                                            # Ensure directory exists
                                            csv_path.parent.mkdir(parents=True, exist_ok=True)

                                            # Write CSV with explicit parameters
                                            df.to_csv(str(csv_path), index=False, encoding='utf-8',
                                                      sep=',', quoting=1, escapechar='\\')

                                            # Verify file was written
                                            if csv_path.exists() and csv_path.stat().st_size > 0:
                                                print(
                                                    f"    ✅ Exported to: {csv_filename} ({csv_path.stat().st_size:,} bytes)")
                                            else:
                                                print(f"    ❌ Failed to write: {csv_filename}")

                                        except Exception as csv_error:
                                            print(f"    ❌ CSV Export Error: {csv_error}")
                                            # Try alternative approach
                                            try:
                                                with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                                                    df.to_csv(csvfile, index=False)
                                                print(f"    ✅ Exported via alternative method: {csv_filename}")
                                            except Exception as alt_error:
                                                print(f"    ❌ Alternative export also failed: {alt_error}")
                                    else:
                                        print(f"    First few rows:")
                                        print(df.head().to_string(max_cols=5))

            except Exception as e:
                print(f"  Error reading {hyper_file.name}: {e}")

        return hyper_data

    def convert_hyper_extracts_to_csv(self):
        """Convert all Hyper extracts (class: hyper, schema: Extract) to CSV files"""
        print(f"\n=== Converting Hyper Extracts to CSV ===")

        # First analyze the workbook to find hyper connections
        datasources = self.analyze_workbook_xml()

        hyper_extract_sources = []
        for ds in datasources:
            for conn in ds['connections']:
                if conn['class'].lower() == 'hyper' and conn['schema'].lower() == 'extract':
                    hyper_extract_sources.append({
                        'datasource': ds['name'],
                        'connection': conn
                    })

        if not hyper_extract_sources:
            print("No Hyper extract connections found in workbook")
            return []

        print(f"Found {len(hyper_extract_sources)} Hyper extract connections:")
        for source in hyper_extract_sources:
            print(f"  - Datasource: {source['datasource']}")

        # Read and convert hyper files to CSV
        return self.read_hyper_files(export_csv=True)

    def analyze_all(self, export_csv=False):
        """Run complete analysis"""
        print("Starting Tableau Workbook Analysis...")

        # Download and extract
        self.download_and_extract()

        # Analyze workbook XML
        datasources = self.analyze_workbook_xml()

        # List data files
        data_files = self.list_data_files()

        # Read hyper files (with optional CSV export)
        hyper_data = self.read_hyper_files(export_csv=export_csv)

        return {
            'datasources': datasources,
            'data_files': data_files,
            'hyper_data': hyper_data
        }


# Usage examples
if __name__ == "__main__":
    # Replace with your actual URL
    url = "https://public.tableau.com/workbooks/DraftRespDash.twb"

    analyzer = TableauWorkbookAnalyzer(url)

    try:
        # Option 1: Full analysis with CSV export
        print("=== RUNNING ANALYSIS WITH CSV EXPORT ===")
        results = analyzer.analyze_all(export_csv=True)

        # Option 2: Just analyze without exporting
        # results = analyzer.analyze_all(export_csv=False)

        # Option 3: Convert only Hyper extracts to CSV
        # analyzer.download_and_extract()
        # hyper_data = analyzer.convert_hyper_extracts_to_csv()

        print(f"\n=== SUMMARY ===")
        print(f"Found {len(results['datasources'])} datasources")
        print(f"Found {len(results['data_files'])} data files")
        print(f"Read {len(results['hyper_data'])} hyper files")

        # Show which datasources use Hyper extracts
        hyper_extract_count = 0
        for ds in results['datasources']:
            for conn in ds['connections']:
                if conn['class'].lower() == 'hyper' and conn['schema'].lower() == 'extract':
                    hyper_extract_count += 1
                    print(f"  Hyper Extract: {ds['name']} -> {conn.get('filename', 'Unknown file')}")

        if hyper_extract_count > 0:
            print(f"\n✅ Converted {hyper_extract_count} Hyper extract(s) to CSV files")
            csv_dir = analyzer.download_dir / "csv_exports"
            print(f"   CSV files saved in: {csv_dir.absolute()}")

            # List the actual CSV files created
            if csv_dir.exists():
                csv_files = list(csv_dir.glob("*.csv"))
                print(f"   Created {len(csv_files)} CSV files:")
                for csv_file in csv_files:
                    print(f"     - {csv_file.name} ({csv_file.stat().st_size:,} bytes)")
            else:
                print(f"   ❌ CSV directory not found: {csv_dir}")
        else:
            print("\n⚠️  No Hyper extract connections found")

    except Exception as e:
        print(f"Error: {e}")
        print("\nNote: Make sure you have the required packages installed:")
        print("pip install requests pandas tableauhyperapi")
