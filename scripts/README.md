# APCD Scripts and Assets

This directory contains scripts and utilities for working with San Diego Air Pollution Control District (APCD) data.

## Scripts

### Upload APCD Files Script

This script uploads APCD files listed in a CSV to S3 bucket at the path `tijuana/sd_apcd_air/raw`.

## Prerequisites

- Python environment with required packages (`minio`, `requests`)
- Environment variables configured (automatically loaded from `workflows/.env`)
- Access to the S3 bucket configured in the environment

## Usage

### Basic Usage

```bash
python scripts/upload_apcd_files.py
```

This will read `data/apcd_sd/apcd_files_list_smart.csv` and upload all files to S3.

### Command Line Options

```bash
python scripts/upload_apcd_files.py [OPTIONS]
```

**Options:**
- `--csv-path PATH`: Path to CSV file with file list (default: `data/apcd_sd/apcd_files_list_smart.csv`)
- `--dry-run`: Simulate upload without actually uploading files
- `--start-from N`: Start processing from row N (0-based indexing)
- `--limit N`: Limit processing to N files
- `--delay SECONDS`: Delay between uploads in seconds (default: 1.0)
- `--verbose, -v`: Enable verbose logging

### Examples

**Test with dry run (recommended first step):**
```bash
python scripts/upload_apcd_files.py --dry-run --limit 5
```

**Upload first 10 files:**
```bash
python scripts/upload_apcd_files.py --limit 10
```

**Resume from row 100:**
```bash
python scripts/upload_apcd_files.py --start-from 100
```

**Upload with faster processing (reduce delay):**
```bash
python scripts/upload_apcd_files.py --delay 0.5
```

**Upload with verbose logging:**
```bash
python scripts/upload_apcd_files.py --verbose --limit 5
```

## CSV Format

The script expects a CSV file with the following columns:
- `Year`: Year folder (e.g., "2023")
- `Month`: Month folder (e.g., "Jan")
- `filename`: Original filename (e.g., "yesterday_20230101.CSV")
- `url`: Source URL to download from

## S3 Path Structure

Files are uploaded to S3 with the following path structure:
```
s3://bucket/tijuana/sd_apcd_air/raw/YEAR/filename
```

For example:
- Source: `yesterday_20230101.CSV`
- S3 Path: `tijuana/sd_apcd_air/raw/2023/yesterday_20230101.CSV`

## Environment Variables

Required environment variables (automatically loaded from `workflows/.env`):
- `S3_BUCKET`: S3 bucket name
- `S3_ADDRESS`: S3 endpoint address
- `S3_ACCESS_KEY`: S3 access key
- `S3_SECRET_KEY`: S3 secret key
- `S3_PORT`: S3 port (optional, default: 443)
- `S3_USE_SSL`: Use SSL (optional, default: true)

## Error Handling

- Failed downloads are logged and skipped
- Network timeouts are handled gracefully
- Script can be interrupted with Ctrl+C
- Progress is logged throughout the process
- Summary report shows success/failure counts

## Performance Considerations

- Default 1-second delay between uploads to avoid overwhelming the source server
- Use `--delay` option to adjust timing
- Use `--limit` for testing or partial uploads
- Use `--start-from` to resume interrupted uploads

## Troubleshooting

**403 Forbidden errors:**
- Source URLs may be protected or no longer accessible
- Check if URLs are still valid
- May need authentication for source server

**Connection errors:**
- Check S3 credentials and endpoint configuration
- Verify network connectivity
- Check firewall settings

**File not found errors:**
- Verify CSV file path is correct
- Check file permissions

## Dagster Assets

### Yearly Aggregation Assets

The APCD data processing pipeline includes two Dagster assets for yearly aggregation:

#### `yearly_aggregated_all`
- **Purpose**: Aggregates all APCD files by year from S3 raw data
- **Input**: Daily files from `test/tijuana/sd_apcd_air/raw/YEAR/`
- **Output**: Yearly aggregated files in `tijuana/sd_apcd_air/output/yearly/all/YEAR/`
- **Formats**: CSV and JSON
- **Schedule**: Weekly (Sundays at 4 AM Pacific Time)

#### `yearly_aggregated_h2s`
- **Purpose**: Filters yearly data to include only H2S measurements
- **Input**: Data from `yearly_aggregated_all` asset
- **Output**: H2S-only yearly files in `tijuana/sd_apcd_air/output/yearly/h2s/YEAR/`
- **Formats**: CSV and JSON
- **Features**: Includes H2S guidance levels (green, yellow, orange, purple)

### Asset Features

- **Automatic Year Processing**: Processes both current year and previous year
- **Error Handling**: Graceful handling of missing files and read errors
- **Progress Logging**: Detailed logging for monitoring aggregation progress
- **Metadata**: Schema.org-compliant metadata for each dataset
- **Multi-format Output**: Stores data in both CSV and JSON formats using `store_assets`

### Asset Usage

The assets are automatically triggered by:
- **Weekly Schedule**: Runs every Sunday at 4 AM Pacific Time
- **AutomationCondition.eager()**: Can also be triggered on-demand

To manually trigger the yearly aggregation:
1. Navigate to Dagster UI
2. Find the "apcd_yearly_aggregation" job
3. Click "Launch Run"

### Data Structure

The aggregated files include:
- All original APCD data columns
- `source_file`: Original daily filename
- `aggregation_year`: Year of the data
- `date_processed`: Timestamp when aggregation was performed
- `level`: H2S guidance level (for H2S assets only)