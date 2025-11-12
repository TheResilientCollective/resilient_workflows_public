# SharePoint Resource Tests

This directory contains comprehensive tests for the SharepointResource class, including unit tests, integration tests, and demo scripts.

## Files Overview

| File | Description |
|------|-------------|
| `test_sharepoint_resource.py` | Main test suite with unit and integration tests |
| `demo_sharepoint_usage.py` | Demo script showing real-world usage |
| `run_tests.py` | Test runner script for easy test execution |
| `pytest.ini` | Pytest configuration |
| `README_sharepoint_tests.md` | This documentation file |

## Quick Start

### 1. Install Requirements

```bash
pip install pytest Office365-REST-Python-Client minio
```

### 2. Set Environment Variables

For integration tests and demo:

```bash
export SHAREPOINT_PASSWORD="your_password_for_dwvalentine@ucsd.edu"
```

For S3/MinIO upload demo (optional):

```bash
export S3_ACCESS_KEY="your_s3_access_key"
export S3_SECRET_KEY="your_s3_secret_key"
export S3_BUCKET="your_bucket_name"
export S3_ADDRESS="your_s3_endpoint"
```

### 3. Run Tests

```bash
# Run unit tests only (fast, no credentials needed)
python run_tests.py unit

# Run integration tests (requires SharePoint credentials)
python run_tests.py integration

# Run all tests
python run_tests.py all

# Run demo script
python run_tests.py demo

# Show help
python run_tests.py help
```

## Test Types

### Unit Tests
- **Fast**: No network calls, all external dependencies mocked
- **Reliable**: Don't require real credentials or network access
- **Coverage**: Test authentication logic, file sorting, error handling

```bash
python -m pytest test_sharepoint_resource.py -m "not integration" -v
```

### Integration Tests
- **Real**: Use actual SharePoint API calls
- **Credentials Required**: Need `SHAREPOINT_PASSWORD` environment variable
- **Network**: Require internet connection to SharePoint

```bash
python -m pytest test_sharepoint_resource.py -m "integration" -v
```

## SharePoint Configuration

The tests are configured for the OIE SharePoint site:

- **Site URL**: `https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/`
- **User**: `dwvalentine@ucsd.edu`
- **Target Folder**: `/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents`
- **Example File**: `infur_20251103.xlsx`

## Test Structure

### TestSharepointResource (Unit Tests)
- `test_sharepoint_resource_initialization()` - Test resource setup
- `test_authenticate_success()` - Test authentication flow
- `test_authenticate_cached()` - Test authentication caching
- `test_latest_sharepoint_dataset_success()` - Test file download
- `test_latest_sharepoint_dataset_no_files()` - Test error handling
- `test_updated_sharepoint_files_success()` - Test bulk file operations
- `test_file_sorting_by_timestamp()` - Test file ordering logic

### TestSharepointResourceIntegration (Integration Tests)
- `test_real_sharepoint_connection()` - Test real authentication
- `test_real_file_download()` - Test real file download

## Demo Script Features

The `demo_sharepoint_usage.py` script demonstrates:

1. **SharePoint Connection**: Authenticate to OIE SharePoint
2. **File Discovery**: Find latest file in Shared Documents
3. **Stream Download**: Download file as BytesIO stream
4. **S3 Upload**: Upload stream directly to S3/MinIO
5. **File Analysis**: Basic file information and type detection

## Running Individual Tests

```bash
# Run specific test class
python -m pytest test_sharepoint_resource.py::TestSharepointResource -v

# Run specific test method
python -m pytest test_sharepoint_resource.py::TestSharepointResource::test_authenticate_success -v

# Run with output capture disabled (see print statements)
python -m pytest test_sharepoint_resource.py -s

# Run with coverage report
python -m pytest test_sharepoint_resource.py --cov=resources.sharepoint
```

## Troubleshooting

### Common Issues

1. **ImportError**: Make sure all packages are installed
   ```bash
   pip install pytest Office365-REST-Python-Client minio
   ```

2. **Authentication Failed**: Check your SharePoint credentials
   ```bash
   echo $SHAREPOINT_PASSWORD  # Should show your password
   ```

3. **No Files Found**: Verify the folder path in SharePoint
   - Check the site URL is correct
   - Verify the folder exists and has files
   - Ensure you have read permissions

4. **S3 Upload Failed**: Check S3/MinIO configuration
   ```bash
   echo $S3_ACCESS_KEY $S3_SECRET_KEY $S3_BUCKET $S3_ADDRESS
   ```

### Debug Mode

Run tests with debug output:

```bash
python -m pytest test_sharepoint_resource.py -v -s --tb=long
```

### Environment Check

Use the demo script to check your environment:

```bash
python demo_sharepoint_usage.py
```

## Integration with Dagster

To use these resources in Dagster assets:

```python
from dagster import asset
from ..resources.sharepoint import SharepointResource
from ..resources.minio import S3Resource

@asset(required_resource_keys={"sharepoint", "s3"})
def latest_oie_data(context):
    sharepoint = context.resources.sharepoint
    s3 = context.resources.s3

    # Download latest file from SharePoint
    stream, filename = sharepoint.latest_sharepoint_dataset(
        "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents"
    )

    # Upload to S3
    s3_path = f"oie_data/{filename}"
    s3.putStream(stream, path=s3_path)

    return s3_path
```

## Test Data

The tests use mock data representing:
- Excel files (`.xlsx`) with timestamps
- File properties from SharePoint API
- Realistic folder structures
- Error conditions (no files, authentication failures)

For integration tests, real files from the OIE SharePoint site are used.

## Contributing

When adding new tests:

1. Add unit tests for new functionality
2. Use mocks for external dependencies
3. Add integration tests for complex workflows
4. Update this README with new test descriptions
5. Follow the existing naming conventions