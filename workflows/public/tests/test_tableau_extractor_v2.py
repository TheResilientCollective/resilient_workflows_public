import pytest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import zipfile
from unittest.mock import Mock, patch, MagicMock
import os
import sys

# Add the utils directory directly to the path to avoid importing full public module
utils_path = '/Users/valentin/development/dev_resilient/resilient_workflows_public/workflows/public/public/utils'
sys.path.insert(0, utils_path)

from tableau_extractor_v2 import TableauWorkbookAnalyzer


class TestTableauWorkbookAnalyzer:
    """Test suite for TableauWorkbookAnalyzer class"""
    
    @pytest.fixture
    def sample_workbook_zip(self):
        """Create a sample workbook zip file for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "test_workbook.zip"
            
            # Create sample files to zip
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                # Add a sample .twb file
                twb_content = '''<?xml version='1.0' encoding='utf-8'?>
<workbook>
    <datasources>
        <datasource name='Demo_Percent+' caption='COVID Data'>
            <connection class='hyper' server='' dbname='' filename='Demo_Percent+ (COVID Flu RSV Tableau Output_2).hyper' schema='Extract'>
            </connection>
            <relation name='Extract' table='[Extract].[Extract]' type='table'>
            </relation>
        </datasource>
    </datasources>
</workbook>'''
                zipf.writestr('DraftRespDash.twb', twb_content)
                
                # Add a placeholder hyper file
                zipf.writestr('Data/Extracts/Demo_Percent+ (COVID Flu RSV Tableau Output_2).hyper', b'mock_hyper_data')
                
                # Add an image file
                zipf.writestr('Image/New Color Bar.png', b'mock_image_data')
            
            yield str(zip_path)
    
    @pytest.fixture
    def analyzer(self):
        """Create a TableauWorkbookAnalyzer instance for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            analyzer = TableauWorkbookAnalyzer(
                url="http://example.com/test.zip",
                download_dir=temp_dir
            )
            yield analyzer
    
    def test_init(self):
        """Test TableauWorkbookAnalyzer initialization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            analyzer = TableauWorkbookAnalyzer(
                url="http://example.com/test.zip",
                download_dir=temp_dir
            )
            
            assert analyzer.url == "http://example.com/test.zip"
            assert analyzer.download_dir == Path(temp_dir)
            assert analyzer.workbook_path is None
            assert analyzer.data_dir is None
            assert analyzer.download_dir.exists()
    
    @patch('tableau_extractor_v2.requests.get')
    def test_download_and_extract(self, mock_get, analyzer, sample_workbook_zip):
        """Test downloading and extracting workbook zip"""
        # Mock the HTTP response
        with open(sample_workbook_zip, 'rb') as f:
            mock_response = Mock()
            mock_response.iter_content.return_value = [f.read()]
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
        
        workbook_path, data_dir = analyzer.download_and_extract()
        
        # Verify download was called
        mock_get.assert_called_once_with(analyzer.url, stream=True)
        
        # Verify files were extracted
        assert workbook_path is not None
        assert workbook_path.exists()
        assert workbook_path.name == 'DraftRespDash.twb'
        
        assert data_dir is not None
        assert data_dir.exists()
        assert data_dir.name == 'Extracts'
    
    def test_analyze_workbook_xml(self, analyzer, sample_workbook_zip):
        """Test XML analysis of workbook file"""
        # Setup analyzer with extracted workbook
        with patch('tableau_extractor_v2.requests.get') as mock_get:
            with open(sample_workbook_zip, 'rb') as f:
                mock_response = Mock()
                mock_response.iter_content.return_value = [f.read()]
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response
            
            analyzer.download_and_extract()
        
        # Test XML analysis
        datasources = analyzer.analyze_workbook_xml()
        
        assert len(datasources) == 1
        assert datasources[0]['name'] == 'Demo_Percent+'
        assert datasources[0]['caption'] == 'COVID Data'
        assert len(datasources[0]['connections']) == 1
        
        connection = datasources[0]['connections'][0]
        assert connection['class'] == 'hyper'
        assert connection['schema'] == 'Extract'
        assert 'Demo_Percent+' in connection['filename']
    
    def test_list_data_files(self, analyzer, sample_workbook_zip):
        """Test listing files in data directory"""
        # Setup analyzer with extracted workbook
        with patch('tableau_extractor_v2.requests.get') as mock_get:
            with open(sample_workbook_zip, 'rb') as f:
                mock_response = Mock()
                mock_response.iter_content.return_value = [f.read()]
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response
            
            analyzer.download_and_extract()
        
        # Test file listing
        data_files = analyzer.list_data_files()
        
        assert len(data_files) == 1
        assert data_files[0]['name'] == 'Demo_Percent+ (COVID Flu RSV Tableau Output_2).hyper'
        assert data_files[0]['extension'] == '.hyper'
        assert data_files[0]['size'] > 0
    
    def test_list_data_files_no_directory(self, analyzer):
        """Test list_data_files when no data directory exists"""
        data_files = analyzer.list_data_files()
        assert data_files == []
    
    @patch('tableau_extractor_v2.HyperProcess')
    def test_read_hyper_files_mock(self, mock_hyper_process, analyzer, sample_workbook_zip):
        """Test reading hyper files with mocked HyperProcess"""
        # Setup analyzer with extracted workbook
        with patch('tableau_extractor_v2.requests.get') as mock_get:
            with open(sample_workbook_zip, 'rb') as f:
                mock_response = Mock()
                mock_response.iter_content.return_value = [f.read()]
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response
            
            analyzer.download_and_extract()
        
        # Mock the Hyper API components
        mock_connection = MagicMock()
        mock_table_def = MagicMock()
        mock_column = MagicMock()
        mock_column.name = 'test_column'
        mock_column.type = 'TEXT'
        mock_table_def.columns = [mock_column]
        
        mock_connection.catalog.get_table_names.return_value = ['Extract.Extract']
        mock_connection.catalog.get_table_definition.return_value = mock_table_def
        
        # Mock query result
        mock_result = MagicMock()
        mock_result.__enter__ = Mock(return_value=mock_result)
        mock_result.__exit__ = Mock(return_value=None)
        mock_result.__iter__ = Mock(return_value=iter([('test_value',)]))
        mock_connection.execute_query.return_value = mock_result
        
        # Setup HyperProcess mock
        mock_hyper_instance = MagicMock()
        mock_hyper_instance.__enter__ = Mock(return_value=mock_hyper_instance)
        mock_hyper_instance.__exit__ = Mock(return_value=None)
        mock_hyper_instance.endpoint = 'mock_endpoint'
        mock_hyper_process.return_value = mock_hyper_instance
        
        # Mock Connection
        with patch('tableau_extractor_v2.Connection') as mock_connection_class:
            mock_connection_class.return_value.__enter__ = Mock(return_value=mock_connection)
            mock_connection_class.return_value.__exit__ = Mock(return_value=None)
            
            # Test reading hyper files
            hyper_data = analyzer.read_hyper_files()
            
            assert len(hyper_data) == 1
            assert hyper_data[0]['file'].endswith('.hyper')
            assert hyper_data[0]['table'] == 'Extract.Extract'
            assert isinstance(hyper_data[0]['dataframe'], pd.DataFrame)
    
    @patch('tableau_extractor_v2.HyperProcess')
    def test_read_hyper_files_with_csv_export(self, mock_hyper_process, analyzer, sample_workbook_zip):
        """Test reading hyper files with CSV export enabled"""
        # Setup analyzer with extracted workbook
        with patch('tableau_extractor_v2.requests.get') as mock_get:
            with open(sample_workbook_zip, 'rb') as f:
                mock_response = Mock()
                mock_response.iter_content.return_value = [f.read()]
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response
            
            analyzer.download_and_extract()
        
        # Mock the Hyper API components (same as above)
        mock_connection = MagicMock()
        mock_table_def = MagicMock()
        mock_column = MagicMock()
        mock_column.name = 'test_column'
        mock_column.type = 'TEXT'
        mock_table_def.columns = [mock_column]
        
        mock_connection.catalog.get_table_names.return_value = ['Extract.Extract']
        mock_connection.catalog.get_table_definition.return_value = mock_table_def
        
        mock_result = MagicMock()
        mock_result.__enter__ = Mock(return_value=mock_result)
        mock_result.__exit__ = Mock(return_value=None)
        mock_result.__iter__ = Mock(return_value=iter([('test_value',)]))
        mock_connection.execute_query.return_value = mock_result
        
        mock_hyper_instance = MagicMock()
        mock_hyper_instance.__enter__ = Mock(return_value=mock_hyper_instance)
        mock_hyper_instance.__exit__ = Mock(return_value=None)
        mock_hyper_instance.endpoint = 'mock_endpoint'
        mock_hyper_process.return_value = mock_hyper_instance
        
        with patch('tableau_extractor_v2.Connection') as mock_connection_class:
            mock_connection_class.return_value.__enter__ = Mock(return_value=mock_connection)
            mock_connection_class.return_value.__exit__ = Mock(return_value=None)
            
            # Test reading hyper files with CSV export
            hyper_data = analyzer.read_hyper_files(export_csv=True)
            
            assert len(hyper_data) == 1
            
            # Check if CSV directory was created
            csv_dir = analyzer.download_dir / "csv_exports"
            assert csv_dir.exists()
    
    def test_read_hyper_files_no_files(self, analyzer):
        """Test read_hyper_files when no hyper files exist"""
        hyper_data = analyzer.read_hyper_files()
        assert hyper_data == []
    
    def test_convert_hyper_extracts_to_csv(self, analyzer, sample_workbook_zip):
        """Test convert_hyper_extracts_to_csv method"""
        # Mock the analyze_workbook_xml and read_hyper_files methods
        mock_datasources = [{
            'name': 'Test_Datasource',
            'connections': [{
                'class': 'hyper',
                'schema': 'Extract',
                'filename': 'test.hyper'
            }]
        }]
        
        mock_hyper_data = [{
            'file': 'test.hyper',
            'table': 'Extract.Extract',
            'dataframe': pd.DataFrame({'col1': [1, 2, 3]}),
            'schema': Mock()
        }]
        
        with patch.object(analyzer, 'analyze_workbook_xml', return_value=mock_datasources):
            with patch.object(analyzer, 'read_hyper_files', return_value=mock_hyper_data):
                result = analyzer.convert_hyper_extracts_to_csv()
                
                assert result == mock_hyper_data
    
    def test_convert_hyper_extracts_to_csv_no_extracts(self, analyzer):
        """Test convert_hyper_extracts_to_csv when no hyper extracts exist"""
        mock_datasources = [{
            'name': 'Test_Datasource',
            'connections': [{
                'class': 'postgres',  # Not hyper
                'schema': 'public',
                'filename': 'test.sql'
            }]
        }]
        
        with patch.object(analyzer, 'analyze_workbook_xml', return_value=mock_datasources):
            result = analyzer.convert_hyper_extracts_to_csv()
            
            assert result == []
    
    def test_analyze_all(self, analyzer, sample_workbook_zip):
        """Test the analyze_all method"""
        mock_datasources = [{'name': 'test'}]
        mock_data_files = [{'name': 'test.hyper'}]
        mock_hyper_data = [{'file': 'test.hyper'}]
        
        with patch.object(analyzer, 'download_and_extract'):
            with patch.object(analyzer, 'analyze_workbook_xml', return_value=mock_datasources):
                with patch.object(analyzer, 'list_data_files', return_value=mock_data_files):
                    with patch.object(analyzer, 'read_hyper_files', return_value=mock_hyper_data):
                        result = analyzer.analyze_all()
                        
                        assert result['datasources'] == mock_datasources
                        assert result['data_files'] == mock_data_files
                        assert result['hyper_data'] == mock_hyper_data


class TestTableauWorkbookAnalyzerIntegration:
    """Integration tests using real workbook file"""
    
    @pytest.fixture
    def real_workbook_path(self):
        """Path to the real workbook.zip file"""
        return "/Users/valentin/development/dev_resilient/resilient_workflows_public/data/sandiego_epideimilogy/workbook.zip"
    
    def test_analyze_real_workbook(self, real_workbook_path):
        """Test analyzing the real workbook.zip file"""
        # Skip if file doesn't exist
        if not Path(real_workbook_path).exists():
            pytest.skip("Real workbook file not found")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Copy the real workbook to temp directory for testing
            temp_workbook = Path(temp_dir) / "workbook.zip"
            shutil.copy2(real_workbook_path, temp_workbook)
            
            # Create analyzer that reads from local file instead of URL
            analyzer = TableauWorkbookAnalyzer(
                url=f"file://{temp_workbook}",
                download_dir=temp_dir
            )
            
            # Mock the download to just copy the local file
            with patch.object(analyzer, 'download_and_extract') as mock_download:
                # Extract the real workbook
                with zipfile.ZipFile(temp_workbook, 'r') as zip_ref:
                    extract_dir = Path(temp_dir) / "extracted"
                    zip_ref.extractall(extract_dir)
                
                # Find workbook and data directory
                for root, dirs, files in os.walk(extract_dir):
                    for file in files:
                        if file.endswith('.twb'):
                            analyzer.workbook_path = Path(root) / file
                    
                    if 'Data' in dirs:
                        analyzer.data_dir = Path(root) / 'Data' / 'Extracts'
                
                mock_download.return_value = (analyzer.workbook_path, analyzer.data_dir)
                
                # Test XML analysis
                datasources = analyzer.analyze_workbook_xml()
                assert len(datasources) > 0
                
                # Test file listing
                data_files = analyzer.list_data_files()
                hyper_files = [f for f in data_files if f['extension'] == '.hyper']
                assert len(hyper_files) > 0
                
                # Test CSV conversion (mocked hyper reading due to dependency complexity)
                with patch.object(analyzer, 'read_hyper_files') as mock_read_hyper:
                    mock_read_hyper.return_value = [{
                        'file': 'Demo_Percent+ (COVID Flu RSV Tableau Output_2).hyper',
                        'table': 'Extract.Extract',
                        'dataframe': pd.DataFrame({
                            'Date': ['2024-01-01', '2024-01-02'],
                            'Value': [10.5, 12.3]
                        }),
                        'schema': Mock()
                    }]
                    
                    result = analyzer.convert_hyper_extracts_to_csv()
                    assert len(result) > 0
    
    def test_extract_and_convert_real_hyper_file(self, real_workbook_path):
        """Test extracting and converting the real hyper file to CSV"""
        # Skip if file doesn't exist
        if not Path(real_workbook_path).exists():
            pytest.skip("Real workbook file not found")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract the workbook directly
            with zipfile.ZipFile(real_workbook_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find the hyper file
            hyper_files = list(Path(temp_dir).rglob("*.hyper"))
            assert len(hyper_files) > 0, "No hyper files found in workbook"
            
            hyper_file = hyper_files[0]
            assert hyper_file.exists(), f"Hyper file {hyper_file} does not exist"
            assert hyper_file.stat().st_size > 0, f"Hyper file {hyper_file} is empty"
            
            # Create analyzer to test CSV conversion
            analyzer = TableauWorkbookAnalyzer(
                url="file://test",
                download_dir=temp_dir
            )
            analyzer.data_dir = hyper_file.parent
            
            # Test that we can at least detect the hyper file
            data_files = analyzer.list_data_files()
            hyper_files_found = [f for f in data_files if f['extension'] == '.hyper']
            assert len(hyper_files_found) > 0
            
            # Mock the hyper reading since tableauhyperapi might not be available
            with patch('tableau_extractor_v2.HyperProcess') as mock_hyper_process:
                mock_connection = MagicMock()
                mock_table_def = MagicMock()
                mock_column = MagicMock()
                mock_column.name = 'Date'
                mock_column.type = 'DATE'
                mock_table_def.columns = [mock_column]
                
                mock_connection.catalog.get_table_names.return_value = ['Extract.Extract']
                mock_connection.catalog.get_table_definition.return_value = mock_table_def
                
                mock_result = MagicMock()
                mock_result.__enter__ = Mock(return_value=mock_result)
                mock_result.__exit__ = Mock(return_value=None)
                mock_result.__iter__ = Mock(return_value=iter([('2024-01-01',)]))
                mock_connection.execute_query.return_value = mock_result
                
                mock_hyper_instance = MagicMock()
                mock_hyper_instance.__enter__ = Mock(return_value=mock_hyper_instance)
                mock_hyper_instance.__exit__ = Mock(return_value=None)
                mock_hyper_instance.endpoint = 'mock_endpoint'
                mock_hyper_process.return_value = mock_hyper_instance
                
                with patch('tableau_extractor_v2.Connection') as mock_connection_class:
                    mock_connection_class.return_value.__enter__ = Mock(return_value=mock_connection)
                    mock_connection_class.return_value.__exit__ = Mock(return_value=None)
                    
                    # Test CSV export
                    hyper_data = analyzer.read_hyper_files(export_csv=True)
                    
                    assert len(hyper_data) > 0
                    assert isinstance(hyper_data[0]['dataframe'], pd.DataFrame)
                    
                    # Check that CSV directory was created
                    csv_dir = Path(temp_dir) / "csv_exports" 
                    assert csv_dir.exists()