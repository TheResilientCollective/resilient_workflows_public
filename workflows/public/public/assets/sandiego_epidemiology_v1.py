import requests
import zipfile
import os
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from tableauhyperapi import HyperProcess, Endpoint, Connection, TableName
import tempfile
import shutil
from dagster import asset, get_dagster_logger, Config
from typing import Dict, List, Any, Optional
import json

from public.resources import s3
from public.utils import store_assets


class TableauWorkbookConfig(Config):
    url: str
    workbook_name: str = "workbook"


@asset(
    required_resource_keys={"s3"},
    automation_condition=None,
    description="Download Tableau workbook from URL and store in S3"
)
def tableau_workbook_download(
    context, 
    config: TableauWorkbookConfig,
    s3: s3
) -> Dict[str, Any]:
    """Download Tableau workbook zip file from URL"""
    logger = get_dagster_logger()
    
    logger.info(f"Downloading Tableau workbook from: {config.url}")
    
    # Download the zip file
    response = requests.get(config.url, stream=True)
    response.raise_for_status()
    
    # Create temporary file to store download
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
        for chunk in response.iter_content(chunk_size=8192):
            temp_file.write(chunk)
        temp_path = temp_file.name
    
    try:
        # Upload to S3
        s3_key = f"tableau/{config.workbook_name}/raw/workbook.zip"
        
        with open(temp_path, 'rb') as f:
            s3.client.put_object(
                Bucket=s3.bucket,
                Key=s3_key,
                Body=f.read(),
                ContentType='application/zip'
            )
        
        file_size = Path(temp_path).stat().st_size
        logger.info(f"Downloaded workbook ({file_size:,} bytes) and stored at s3://{s3.bucket}/{s3_key}")
        
        return {
            "s3_key": s3_key,
            "file_size": file_size,
            "url": config.url,
            "workbook_name": config.workbook_name
        }
        
    finally:
        # Clean up temp file
        os.unlink(temp_path)


@asset(
    deps=[tableau_workbook_download],
    required_resource_keys={"s3"},
    automation_condition=None,
    description="Extract and analyze Tableau workbook contents"
)
def tableau_workbook_analysis(
    context,
    tableau_workbook_download: Dict[str, Any],
    s3: s3
) -> Dict[str, Any]:
    """Extract and analyze Tableau workbook zip contents"""
    logger = get_dagster_logger()
    
    workbook_name = tableau_workbook_download["workbook_name"]
    s3_key = tableau_workbook_download["s3_key"]
    
    logger.info(f"Analyzing Tableau workbook: {workbook_name}")
    
    # Create temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download zip file from S3
        zip_path = Path(temp_dir) / "workbook.zip"
        
        with open(zip_path, 'wb') as f:
            s3.client.download_fileobj(s3.bucket, s3_key, f)
        
        # Extract the zip file
        extract_dir = Path(temp_dir) / "extracted"
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        logger.info(f"Extracted workbook to: {extract_dir}")
        
        # Find the .twb file and data directory
        workbook_path = None
        data_dir = None
        
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.twb'):
                    workbook_path = Path(root) / file
                    logger.info(f"Found workbook: {workbook_path}")
            
            if 'Data' in dirs:
                data_dir = Path(root) / 'Data'
                logger.info(f"Found data directory: {data_dir}")
        
        # Analyze workbook XML
        datasources = []
        if workbook_path and workbook_path.exists():
            datasources = _analyze_workbook_xml(workbook_path, logger)
        
        # List data files
        data_files = []
        if data_dir and data_dir.exists():
            data_files = _list_data_files(data_dir, logger)
        
        # Find hyper files
        hyper_files = [f for f in data_files if f['extension'] == '.hyper']
        
        # Store analysis results
        analysis_result = {
            "workbook_name": workbook_name,
            "datasources": datasources,
            "data_files": data_files,
            "hyper_files": len(hyper_files),
            "total_files": len(data_files)
        }
        
        # Store analysis as JSON
        analysis_key = f"tableau/{workbook_name}/analysis/workbook_analysis.json"
        s3.client.put_object(
            Bucket=s3.bucket,
            Key=analysis_key,
            Body=json.dumps(analysis_result, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Analysis complete: {len(datasources)} datasources, {len(data_files)} data files")
        
        return analysis_result


@asset(
    deps=[tableau_workbook_analysis],
    required_resource_keys={"s3"},
    automation_condition=None,
    description="Extract Hyper files from Tableau workbook and convert to CSV"
)
def tableau_hyper_extraction(
    context,
    tableau_workbook_download: Dict[str, Any],
    tableau_workbook_analysis: Dict[str, Any],
    s3: s3
) -> Dict[str, Any]:
    """Extract Hyper files and convert to CSV format"""
    logger = get_dagster_logger()
    
    workbook_name = tableau_workbook_download["workbook_name"]
    s3_key = tableau_workbook_download["s3_key"]
    
    if tableau_workbook_analysis["hyper_files"] == 0:
        logger.info("No Hyper files found in workbook")
        return {"hyper_files_processed": 0, "csv_files_created": 0}
    
    logger.info(f"Extracting {tableau_workbook_analysis['hyper_files']} Hyper files")
    
    # Create temporary directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download and extract workbook again
        zip_path = Path(temp_dir) / "workbook.zip"
        
        with open(zip_path, 'wb') as f:
            s3.client.download_fileobj(s3.bucket, s3_key, f)
        
        extract_dir = Path(temp_dir) / "extracted"
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Find hyper files
        hyper_files = list(extract_dir.rglob("*.hyper"))
        
        csv_files_created = 0
        hyper_data = []
        
        for hyper_file in hyper_files:
            logger.info(f"Processing Hyper file: {hyper_file.name}")
            
            try:
                # Extract data from Hyper file
                extracted_data = _extract_hyper_data(hyper_file, logger)
                
                for table_data in extracted_data:
                    # Convert to CSV and store in S3
                    df = table_data['dataframe']
                    
                    # Clean table name for filename
                    clean_table_name = str(table_data['table']).replace('"', '').replace('.', '_').replace(' ', '_')
                    csv_filename = f"{hyper_file.stem}_{clean_table_name}.csv"
                    
                    # Convert DataFrame to CSV
                    csv_buffer = df.to_csv(index=False)
                    
                    # Store in S3
                    csv_key = f"tableau/{workbook_name}/csv/{csv_filename}"
                    s3.client.put_object(
                        Bucket=s3.bucket,
                        Key=csv_key,
                        Body=csv_buffer.encode('utf-8'),
                        ContentType='text/csv'
                    )
                    
                    csv_files_created += 1
                    logger.info(f"Created CSV: {csv_filename} ({len(df)} rows, {len(df.columns)} columns)")
                    
                    hyper_data.append({
                        "hyper_file": hyper_file.name,
                        "table": table_data['table'],
                        "csv_key": csv_key,
                        "rows": len(df),
                        "columns": len(df.columns)
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {hyper_file.name}: {e}")
        
        # Store extraction results
        extraction_result = {
            "workbook_name": workbook_name,
            "hyper_files_processed": len(hyper_files),
            "csv_files_created": csv_files_created,
            "extracted_data": hyper_data
        }
        
        # Store results as JSON
        results_key = f"tableau/{workbook_name}/extraction/hyper_extraction_results.json"
        s3.client.put_object(
            Bucket=s3.bucket,
            Key=results_key,
            Body=json.dumps(extraction_result, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Hyper extraction complete: {csv_files_created} CSV files created")
        
        return extraction_result


def _analyze_workbook_xml(workbook_path: Path, logger) -> List[Dict[str, Any]]:
    """Analyze the .twb file to find data sources"""
    logger.info(f"Analyzing workbook XML: {workbook_path.name}")
    
    # Parse the XML
    tree = ET.parse(workbook_path)
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
    
    logger.info(f"Found {len(datasources)} datasources")
    return datasources


def _list_data_files(data_dir: Path, logger) -> List[Dict[str, Any]]:
    """List all files in the data directory"""
    logger.info(f"Listing files in data directory: {data_dir}")
    
    data_files = []
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            file_path = Path(root) / file
            file_size = file_path.stat().st_size
            data_files.append({
                'path': str(file_path),
                'name': file,
                'size': file_size,
                'extension': file_path.suffix
            })
            logger.info(f"  {file} ({file_size:,} bytes)")
    
    return data_files


def _extract_hyper_data(hyper_file: Path, logger) -> List[Dict[str, Any]]:
    """Extract data from Tableau Hyper file"""
    logger.info(f"Extracting data from: {hyper_file.name}")
    
    hyper_data = []
    
    try:
        with HyperProcess(telemetry=Endpoint.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=str(hyper_file)) as connection:
                # Get all table names
                table_names = connection.catalog.get_table_names("Extract")
                
                for table_name in table_names:
                    logger.info(f"  Processing table: {table_name}")
                    
                    # Get table definition
                    table_def = connection.catalog.get_table_definition(table_name)
                    
                    # Read ALL data
                    query = f'SELECT * FROM {table_name}'
                    
                    with connection.execute_query(query) as result:
                        rows = list(result)
                        logger.info(f"    Read {len(rows)} rows")
                        
                        if rows:
                            # Convert to DataFrame
                            columns = [col.name for col in table_def.columns]
                            df = pd.DataFrame(rows, columns=columns)
                            
                            hyper_data.append({
                                'table': str(table_name),
                                'dataframe': df,
                                'rows': len(df),
                                'columns': len(df.columns)
                            })
                            
                            logger.info(f"    DataFrame shape: {df.shape}")
                            
    except Exception as e:
        logger.error(f"Error reading {hyper_file.name}: {e}")
        # Return empty list on error
        return []
    
    return hyper_data