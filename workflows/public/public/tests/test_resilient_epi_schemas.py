"""
Test file for Resilient Epidemiology Schemas

This file tests the Pandera-based epidemiology data schemas to ensure
they work correctly before integration into assets.
"""

import pytest
import pandas as pd
from datetime import datetime, date
import sys
import os

# Add the parent directory to sys.path to import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    StatisticalExtensionSchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    transform_to_basic_epidemiology,
    validate_basic_epidemiology_format,
    validate_statistical_extension_format,
    create_statistical_extension_record
)


class TestBasicEpidemiologySchema:
    """Test the basic epidemiology schema"""

    def test_valid_basic_schema(self):
        """Test validation with valid basic epidemiology data"""
        valid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego', 'LosAngeles', 'Orange'],
            'date_week_start': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'date_week_end': ['2024-01-08', '2024-01-15', '2024-01-22'],
            'Week_Number': [1, 2, 3],
            'Year': [2024, 2024, 2024],
            'Week_Year': ['1-2024', '2-2024', '3-2024'],
            'Cases': [10, 15, 8]
        })

        # Should not raise an exception
        result = BasicEpidemiologySchema.validate(valid_data)
        assert len(result) == 3
        assert result.equals(valid_data)

    def test_invalid_jurisdiction_with_spaces(self):
        """Test validation fails with spaces in jurisdiction"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['San Diego'],  # Has space - should fail
            'date_week_start': ['2024-01-01'],
            'date_week_end': ['2024-01-08'],
            'Week_Number': [1],
            'Year': [2024],
            'Week_Year': ['1-2024'],
            'Cases': [10]
        })

        with pytest.raises(EpidemiologyValidationError):
            BasicEpidemiologySchema.validate(invalid_data)

    def test_invalid_date_format(self):
        """Test validation fails with invalid date format"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date_week_start': ['01/01/2024'],  # Wrong format - should fail
            'date_week_end': ['2024-01-08'],
            'Week_Number': [1],
            'Year': [2024],
            'Week_Year': ['1-2024'],
            'Cases': [10]
        })

        with pytest.raises(EpidemiologyValidationError):
            BasicEpidemiologySchema.validate(invalid_data)

    def test_negative_cases(self):
        """Test validation fails with negative cases"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date_week_start': ['2024-01-01'],
            'date_week_end': ['2024-01-08'],
            'Week_Number': [1],
            'Year': [2024],
            'Week_Year': ['1-2024'],
            'Cases': [-5]  # Negative - should fail
        })

        with pytest.raises(EpidemiologyValidationError):
            BasicEpidemiologySchema.validate(invalid_data)

    def test_transform_from_source(self):
        """Test transformation from source data format"""
        source_data = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'Count': [10, 15, 8]
        })

        result = BasicEpidemiologySchema.transform_from_source(source_data, 'TestJurisdiction')

        # Check structure
        expected_columns = ['Jurisdiction', 'date_week_start', 'date_week_end',
                           'Week_Number', 'Year', 'Week_Year', 'Cases']
        assert list(result.columns) == expected_columns

        # Check values
        assert result['Jurisdiction'].iloc[0] == 'TestJurisdiction'
        assert result['Cases'].tolist() == [10, 15, 8]
        assert result['date_week_start'].iloc[0] == '2024-01-01'

    def test_transform_empty_dataframe(self):
        """Test transformation with empty dataframe"""
        empty_df = pd.DataFrame()
        result = BasicEpidemiologySchema.transform_from_source(empty_df, 'Test')
        assert len(result) == 0
        assert list(result.columns) == list(BasicEpidemiologySchema.schema.columns.keys())


class TestStatisticalExtensionSchema:
    """Test the statistical extension schema"""

    def test_valid_statistical_extension(self):
        """Test validation with valid statistical extension data"""
        valid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['cases'],
            'observation_type': ['actual'],
            'count': [50],
            'mean': [48.5],
            'lower_ci': [45.0],
            'upper_ci': [52.0]
        })

        result = StatisticalExtensionSchema.validate(valid_data)
        assert len(result) == 1

    def test_invalid_metric(self):
        """Test validation fails with invalid metric"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['invalid_metric'],  # Not in allowed list
            'observation_type': ['actual'],
            'count': [50]
        })

        with pytest.raises(EpidemiologyValidationError):
            StatisticalExtensionSchema.validate(invalid_data)

    def test_missing_optional_fields(self):
        """Test validation fails when no optional fields are present"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['cases'],
            'observation_type': ['actual']
            # No optional fields - should fail
        })

        with pytest.raises(EpidemiologyValidationError):
            StatisticalExtensionSchema.validate(invalid_data)

    def test_paired_fields_validation(self):
        """Test paired fields must both be present or both absent"""
        # Only lower_ci without upper_ci - should fail
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['cases'],
            'observation_type': ['actual'],
            'count': [50],
            'lower_ci': [45.0]
            # Missing upper_ci - should fail
        })

        with pytest.raises(EpidemiologyValidationError):
            StatisticalExtensionSchema.validate(invalid_data)

    def test_create_record(self):
        """Test creating a single record"""
        result = StatisticalExtensionSchema.create_record(
            jurisdiction='TestJurisdiction',
            date='2024-01-01',
            disease='Mpox',
            metric='cases',
            observation_type='actual',
            count=25,
            mean=24.5
        )

        assert len(result) == 1
        assert result['Jurisdiction'].iloc[0] == 'TestJurisdiction'
        assert result['disease'].iloc[0] == 'Mpox'
        assert result['count'].iloc[0] == 25


class TestResilientEpiProcessor:
    """Test the main processor class"""

    def test_process_basic_epidemiology_data(self):
        """Test processing basic epidemiology data"""
        processor = ResilientEpiProcessor()

        source_data = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-08'],
            'Count': [10, 15]
        })

        result = processor.process_basic_epidemiology_data(
            source_data,
            jurisdiction='TestJurisdiction'
        )

        assert len(result) == 2
        assert 'Jurisdiction' in result.columns
        assert result['Cases'].tolist() == [10, 15]

    def test_process_statistical_extension_data(self):
        """Test processing statistical extension data"""
        processor = ResilientEpiProcessor()

        stat_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['cases'],
            'observation_type': ['actual'],
            'count': [50]
        })

        result = processor.process_statistical_extension_data(stat_data)
        assert len(result) == 1
        assert result['disease'].iloc[0] == 'COVID-19'


class TestConvenienceFunctions:
    """Test convenience functions"""

    def test_transform_to_basic_epidemiology(self):
        """Test the convenience transform function"""
        source_data = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Count': [10]
        })

        result = transform_to_basic_epidemiology(source_data, 'Test')
        assert result['Jurisdiction'].iloc[0] == 'Test'
        assert result['Cases'].iloc[0] == 10

    def test_create_statistical_extension_record(self):
        """Test the convenience record creation function"""
        result = create_statistical_extension_record(
            jurisdiction='Test',
            date=datetime(2024, 1, 1),
            disease='Mpox',
            metric='cases',
            observation_type='forecast',
            mean=15.5,
            lower_ci=10.0,
            upper_ci=21.0
        )

        assert len(result) == 1
        assert result['date'].iloc[0] == '2024-01-01'
        assert result['mean'].iloc[0] == 15.5


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_invalid_input_missing_columns(self):
        """Test error with missing required input columns"""
        invalid_source = pd.DataFrame({
            'Date': ['2024-01-01'],
            # Missing 'Count' column
        })

        with pytest.raises(EpidemiologyValidationError):
            BasicEpidemiologySchema.transform_from_source(invalid_source)

    def test_invalid_dates_filtered_out(self):
        """Test that invalid dates are filtered out during transformation"""
        source_with_bad_dates = pd.DataFrame({
            'Date': ['2024-01-01', 'invalid-date', '2024-01-08'],
            'Count': [10, 15, 20]
        })

        result = BasicEpidemiologySchema.transform_from_source(source_with_bad_dates)
        # Should only have 2 rows (invalid date filtered out)
        assert len(result) == 2
        assert result['Cases'].tolist() == [10, 20]

    def test_statistical_extension_range_validation(self):
        """Test that lower bound <= upper bound validation works"""
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['cases'],
            'observation_type': ['actual'],
            'count': [50],
            'lower_ci': [60.0],  # Lower > Upper - should fail
            'upper_ci': [50.0]
        })

        with pytest.raises(EpidemiologyValidationError):
            StatisticalExtensionSchema.validate(invalid_data)


if __name__ == "__main__":
    # Run tests directly
    print("Running Resilient Epi Schemas Tests...")

    # Test basic schema
    print("\n1. Testing Basic Epidemiology Schema...")
    try:
        test_basic = TestBasicEpidemiologySchema()
        test_basic.test_valid_basic_schema()
        test_basic.test_transform_from_source()
        print("✅ Basic schema tests passed")
    except Exception as e:
        print(f"❌ Basic schema tests failed: {e}")

    # Test statistical extension schema
    print("\n2. Testing Statistical Extension Schema...")
    try:
        test_stat = TestStatisticalExtensionSchema()
        test_stat.test_valid_statistical_extension()
        test_stat.test_create_record()
        print("✅ Statistical extension tests passed")
    except Exception as e:
        print(f"❌ Statistical extension tests failed: {e}")

    # Test processor
    print("\n3. Testing ResilientEpiProcessor...")
    try:
        test_processor = TestResilientEpiProcessor()
        test_processor.test_process_basic_epidemiology_data()
        test_processor.test_process_statistical_extension_data()
        print("✅ Processor tests passed")
    except Exception as e:
        print(f"❌ Processor tests failed: {e}")

    # Test convenience functions
    print("\n4. Testing Convenience Functions...")
    try:
        test_convenience = TestConvenienceFunctions()
        test_convenience.test_transform_to_basic_epidemiology()
        test_convenience.test_create_statistical_extension_record()
        print("✅ Convenience function tests passed")
    except Exception as e:
        print(f"❌ Convenience function tests failed: {e}")

    print("\n🎉 All tests completed!")