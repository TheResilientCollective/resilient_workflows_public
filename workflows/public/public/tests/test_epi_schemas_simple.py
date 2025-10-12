"""
Simple test file for Resilient Epidemiology Schemas

This file tests the Pandera-based epidemiology data schemas without pytest dependency.
"""

import pandas as pd
from datetime import datetime, date
import sys
import os

# Add the parent directory to sys.path to import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
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
    print("✅ Successfully imported resilient_epi_schemas module")
except ImportError as e:
    print(f"❌ Failed to import resilient_epi_schemas: {e}")
    sys.exit(1)


def test_basic_epidemiology_schema():
    """Test the basic epidemiology schema"""
    print("\n=== Testing Basic Epidemiology Schema ===")

    # Test 1: Valid data
    print("Test 1: Valid basic epidemiology data...")
    try:
        valid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego', 'LosAngeles', 'Orange'],
            'date_week_start': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'date_week_end': ['2024-01-08', '2024-01-15', '2024-01-22'],
            'Week_Number': [1, 2, 3],
            'Year': [2024, 2024, 2024],
            'Week_Year': ['1-2024', '2-2024', '3-2024'],
            'Cases': [10, 15, 8]
        })

        result = BasicEpidemiologySchema.validate(valid_data)
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
        print("✅ Valid data test passed")
    except Exception as e:
        print(f"❌ Valid data test failed: {e}")
        return False

    # Test 2: Transform from source
    print("Test 2: Transform from source data...")
    try:
        source_data = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'Count': [10, 15, 8]
        })

        result = BasicEpidemiologySchema.transform_from_source(source_data, 'TestJurisdiction')

        # Check structure (verify all expected columns are present)
        expected_columns = ['Jurisdiction', 'date_week_start', 'date_week_end',
                           'Week_Number', 'Year', 'Week_Year', 'Cases']
        assert set(result.columns) == set(expected_columns), f"Column mismatch. Expected: {expected_columns}, Got: {list(result.columns)}"
        assert result['Jurisdiction'].iloc[0] == 'TestJurisdiction'
        assert result['Cases'].tolist() == [10, 15, 8]
        print("✅ Transform test passed")
    except Exception as e:
        print(f"❌ Transform test failed: {e}")
        return False

    # Test 3: Invalid data (jurisdiction with spaces)
    print("Test 3: Invalid jurisdiction with spaces...")
    try:
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['San Diego'],  # Has space - should fail
            'date_week_start': ['2024-01-01'],
            'date_week_end': ['2024-01-08'],
            'Week_Number': [1],
            'Year': [2024],
            'Week_Year': ['1-2024'],
            'Cases': [10]
        })

        BasicEpidemiologySchema.validate(invalid_data)
        print("❌ Should have failed with spaces in jurisdiction")
        return False
    except EpidemiologyValidationError:
        print("✅ Correctly caught invalid jurisdiction with spaces")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

    return True


def test_statistical_extension_schema():
    """Test the statistical extension schema"""
    print("\n=== Testing Statistical Extension Schema ===")

    # Test 1: Valid data
    print("Test 1: Valid statistical extension data...")
    try:
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
        print("✅ Valid statistical extension data test passed")
    except Exception as e:
        print(f"❌ Valid statistical extension data test failed: {e}")
        return False

    # Test 2: Create record
    print("Test 2: Create statistical extension record...")
    try:
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
        print("✅ Create record test passed")
    except Exception as e:
        print(f"❌ Create record test failed: {e}")
        return False

    # Test 3: Invalid metric
    print("Test 3: Invalid metric...")
    try:
        invalid_data = pd.DataFrame({
            'Jurisdiction': ['SanDiego'],
            'date': ['2024-01-01'],
            'disease': ['COVID-19'],
            'metric': ['invalid_metric'],  # Not in allowed list
            'observation_type': ['actual'],
            'count': [50]
        })

        StatisticalExtensionSchema.validate(invalid_data)
        print("❌ Should have failed with invalid metric")
        return False
    except EpidemiologyValidationError:
        print("✅ Correctly caught invalid metric")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

    return True


def test_processor():
    """Test the ResilientEpiProcessor"""
    print("\n=== Testing ResilientEpiProcessor ===")

    try:
        processor = ResilientEpiProcessor()

        # Test basic epidemiology processing
        print("Test 1: Process basic epidemiology data...")
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
        print("✅ Basic epidemiology processing test passed")

        # Test statistical extension processing
        print("Test 2: Process statistical extension data...")
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
        print("✅ Statistical extension processing test passed")

        return True
    except Exception as e:
        print(f"❌ Processor test failed: {e}")
        return False


def test_convenience_functions():
    """Test convenience functions"""
    print("\n=== Testing Convenience Functions ===")

    try:
        # Test transform convenience function
        print("Test 1: Transform convenience function...")
        source_data = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Count': [10]
        })

        result = transform_to_basic_epidemiology(source_data, 'Test')
        assert result['Jurisdiction'].iloc[0] == 'Test'
        assert result['Cases'].iloc[0] == 10
        print("✅ Transform convenience function test passed")

        # Test create record convenience function
        print("Test 2: Create record convenience function...")
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
        print("✅ Create record convenience function test passed")

        return True
    except Exception as e:
        print(f"❌ Convenience functions test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 Running Resilient Epi Schemas Tests...")

    tests = [
        test_basic_epidemiology_schema,
        test_statistical_extension_schema,
        test_processor,
        test_convenience_functions
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test_func.__name__} had unexpected error: {e}")

    print(f"\n🎯 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The schemas are working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)