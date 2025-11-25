# APCD File List Generator

This directory contains scripts to generate lists of APCD data files from the website `http://jtimmer.digitalspacemail17.net/data/` with the naming convention `yesterday_{YYYYmmdd}.CSV`.

## Enhanced Features

The enhanced script handles the reality that **the latest data may be split** between different locations:
- **Latest data** (last 30 days) is at the top level: `http://domain/data/yesterday_YYYYMMDD.CSV`
- **Current year data** may be partially in yearly directories: `http://domain/data/YYYY/MMM/yesterday_YYYYMMDD.CSV`
- **Historical data** is in yearly/monthly directories

## Scripts

### 1. `generate_apcd_file_list_with_check.py` (Recommended)

Enhanced script with smart URL logic and optional availability checking.

**Features:**
- **Smart URL logic** - Automatically determines correct URL structure based on date
- **URL accessibility checking** - Can test if files are actually accessible
- **Concurrent checking** - Fast parallel URL testing
- **Flexible output** - CSV format with optional status column

**Usage:**
```bash
# Generate theoretical file list (fast)
python scripts/generate_apcd_file_list_with_check.py --start-year 2023 --output apcd_files.csv

# Check URL accessibility (slower but accurate)
python scripts/generate_apcd_file_list_with_check.py --start-year 2023 --check-urls --output apcd_files_checked.csv

# Generate for specific year range
python scripts/generate_apcd_file_list_with_check.py --start-year 2024 --end-year 2024 --include-status

# Fast checking with more workers
python scripts/generate_apcd_file_list_with_check.py --check-urls --workers 20

# Only files in yearly/monthly directories (not top level)
python scripts/generate_apcd_file_list_with_check.py --non-top-level-only --start-year 2023

# Check availability of only non-top-level files
python scripts/generate_apcd_file_list_with_check.py --non-top-level-only --check-urls --output non_top_level_checked.csv
```

**Arguments:**
- `--start-year YYYY`: Starting year (default: 2023)
- `--end-year YYYY`: Ending year (default: current year)
- `--output FILE`: Output CSV file (default: stdout)
- `--check-urls`: Check URL accessibility (slower)
- `--workers N`: Concurrent workers for URL checking (default: 10)
- `--include-status`: Include status column even without URL checking
- `--non-top-level-only`: Only include files NOT in the top level directory

### 2. `generate_apcd_file_list.py`

Original script with web scraping capability (may not work due to access restrictions).

## URL Logic

The enhanced script uses intelligent URL determination:

### Recent Files (last 30 days)
```
http://jtimmer.digitalspacemail17.net/data/yesterday_20251121.CSV
```

### Current Year Files (older than 30 days)
```
# Recent current year (last 90 days)
http://jtimmer.digitalspacemail17.net/data/yesterday_20250823.CSV

# Older current year
http://jtimmer.digitalspacemail17.net/data/2025/Jan/yesterday_20250115.CSV
```

### Historical Files
```
http://jtimmer.digitalspacemail17.net/data/2023/Jan/yesterday_20230101.CSV
http://jtimmer.digitalspacemail17.net/data/2024/Dec/yesterday_20241225.CSV
```

## Output Format

### Basic Output
```csv
Year,Month,filename,url
2023,Jan,yesterday_20230101.CSV,http://jtimmer.digitalspacemail17.net/data/2023/Jan/yesterday_20230101.CSV
2025,Nov,yesterday_20251121.CSV,http://jtimmer.digitalspacemail17.net/data/yesterday_20251121.CSV
```

### With Status Checking
```csv
Year,Month,filename,url,status
2023,Jan,yesterday_20230101.CSV,http://...,FORBIDDEN
2025,Nov,yesterday_20251121.CSV,http://...,OK
```

**Status Values:**
- `OK`: File is accessible (HTTP 200)
- `FORBIDDEN`: Access denied (HTTP 403)
- `NOT_FOUND`: File not found (HTTP 404)
- `TIMEOUT`: Request timed out
- `ERROR`: Other network error
- `THEORETICAL`: URL not tested

## Filtering Options

### Non-Top-Level Files Only

Use `--non-top-level-only` to filter for files that are NOT in the top level directory. This is useful for:

- **Historical data analysis** - Focus on files in yearly/monthly directory structure
- **Archive validation** - Check availability of older files that should be in organized directories
- **Reduced checking** - Faster URL validation by excluding recent top-level files

**Examples:**
```bash
# Get only files in yearly/monthly structure (2023: 365 files vs 1,056 total)
python scripts/generate_apcd_file_list_with_check.py --non-top-level-only --start-year 2023

# Check just the organized archive files
python scripts/generate_apcd_file_list_with_check.py --non-top-level-only --check-urls
```

**File Count Breakdown (2023-current):**
- **Total files**: 1,056
- **Non-top-level (yearly/monthly)**: 965
- **Top-level (recent)**: 91

## Generated Files

- **`apcd_files_list_smart.csv`**: Complete file list from 2023 to current with smart URL logic (1,056+ entries)
- **`apcd_files_list.csv`**: Original theoretical file list

## Performance

- **Theoretical generation**: ~1,056 files in <1 second
- **URL checking**: ~1,056 files in ~30 seconds with 10 workers
- **Memory usage**: Minimal - processes files incrementally

## Access Considerations

The website may have access restrictions (403 Forbidden responses). The scripts handle this gracefully:

1. **Generate theoretical URLs** - Always works, provides expected file locations
2. **Check accessibility** - Tests if files can actually be downloaded
3. **Smart fallbacks** - Handles network errors and access restrictions

This approach ensures you always get a useful file list, even when the website has access restrictions.
