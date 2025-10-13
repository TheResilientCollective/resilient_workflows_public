"""
Resilient Epidemiology Data Schemas

This module provides Pandera-based data schemas and validation for epidemiology data output,
supporting both basic epidemiology format and statistical extension format
as specified in the epidemiology data plan.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
from dagster import get_dagster_logger
import re


class EpidemiologyValidationError(Exception):
    """Custom exception for epidemiology data validation errors"""
    pass


# Basic Epidemiology Schema (formerly San Diego County model)
basic_epidemiology_schema = DataFrameSchema({
    "Jurisdiction": Column(
        str,
        checks=[
            Check(lambda s: s.str.contains(' ').sum() == 0,
                  error="Jurisdiction must not contain spaces (should be CamelCased)"),
            Check(lambda s: s.str.len() > 0,
                  error="Jurisdiction cannot be empty")
        ],
        description="Geographic jurisdiction (CamelCased, no spaces)"
    ),
    "date_week_start": Column(
        str,
        checks=[
            Check(lambda s: s.str.match(r'^\d{4}-\d{2}-\d{2}$').all(),
                  error="date_week_start must be in YYYY-mm-dd format")
        ],
        description="Week start date in YYYY-mm-dd format"
    ),
    "date_week_end": Column(
        str,
        checks=[
            Check(lambda s: s.str.match(r'^\d{4}-\d{2}-\d{2}$').all(),
                  error="date_week_end must be in YYYY-mm-dd format")
        ],
        description="Week end date in YYYY-mm-dd format (start + 7 days)"
    ),
    "Week_Number": Column(
        int,
        checks=[
            Check.in_range(1, 53, include_min=True, include_max=True),
        ],
        description="ISO week number (1-53)"
    ),
    "Year": Column(
        int,
        checks=[
            Check.in_range(2000, 2100, include_min=True, include_max=True),
        ],
        description="Year"
    ),
    "Week_Year": Column(
        str,
        checks=[
            Check(lambda s: s.str.match(r'^\d+-\d{4}$').all(),
                  error="Week_Year must be in format 'WeekNumber-Year'")
        ],
        description="Combined week-year identifier (WeekNumber-Year)"
    ),
    "Cases": Column(
        int,
        checks=[
            Check.greater_than_or_equal_to(0),
        ],
        description="Number of cases (non-negative integer)"
    ),
}, strict=True, description="Basic epidemiology data schema")


# Statistical Extension Schema - Base required columns only
statistical_extension_schema_base = DataFrameSchema({
    "Jurisdiction": Column(
        str,
        checks=[Check(lambda s: s.str.len() > 0, error="Jurisdiction cannot be empty")],
        description="Geographic jurisdiction"
    ),
    "date": Column(
        str,
        checks=[
            Check(lambda s: s.str.match(r'^\d{4}-\d{2}-\d{2}$').all(),
                  error="date must be in YYYY-mm-dd format")
        ],
        description="Date in ISO format (YYYY-mm-dd)"
    ),
    "disease": Column(
        str,
        checks=[Check(lambda s: s.str.len() > 0, error="Disease cannot be empty")],
        description="Name of the pathogen/disease"
    ),
    "metric": Column(
        str,
        checks=[
            Check.isin(['cases', 'deaths', 'hospitalizations', 'tests', 'vaccinations']),
        ],
        description="Type of metric being reported"
    ),
    "observation_type": Column(
        str,
        checks=[
            Check.isin(['actual', 'partial-data estimate', 'prediction', 'forecast']),
        ],
        description="Type of observation"
    ),
}, strict=False, description="Statistical extension epidemiology data schema (base)")

# Dictionary of optional column definitions for validation when present
OPTIONAL_COLUMNS = {
    "mean": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "count": Column(int, checks=[Check.greater_than_or_equal_to(0)]),
    "rate": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "median": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "lower_ci": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "upper_ci": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "lower_20": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "upper_20": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "lower_50": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "upper_50": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "lower_90": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
    "upper_90": Column(float, checks=[Check.greater_than_or_equal_to(0)]),
}


# Add custom dataframe-level checks for statistical extension schema
@pa.check_types
def validate_statistical_extension_requirements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate business rules for statistical extension schema:
    1. At least one of mean, count, rate, median must be present (non-null)
    2. Paired fields must both be present or both absent
    """
    # Check 1: At least one optional_1 field must be non-null
    optional_1_fields = ['mean', 'count', 'rate', 'median']
    available_optional_1 = [col for col in optional_1_fields if col in df.columns]

    if not available_optional_1:
        raise EpidemiologyValidationError(
            f"At least one of {optional_1_fields} columns must be present"
        )

    has_values = df[available_optional_1].notna().any(axis=1)
    if not has_values.all():
        null_rows = df[~has_values].index.tolist()
        raise EpidemiologyValidationError(
            f"Rows {null_rows} have no values in any of {available_optional_1}"
        )

    # Check 2: Paired fields validation
    pairs = [
        ('lower_ci', 'upper_ci'),
        ('lower_20', 'upper_20'),
        ('lower_50', 'upper_50'),
        ('lower_90', 'upper_90')
    ]

    for lower_col, upper_col in pairs:
        if lower_col in df.columns and upper_col in df.columns:
            # Both present - check that if one has value, other also has value
            lower_has_value = df[lower_col].notna()
            upper_has_value = df[upper_col].notna()
            mismatch = lower_has_value != upper_has_value
            if mismatch.any():
                mismatch_rows = df[mismatch].index.tolist()
                raise EpidemiologyValidationError(
                    f"Paired fields {lower_col}/{upper_col} must both have values or both be null. "
                    f"Mismatched rows: {mismatch_rows}"
                )

            # Validate that lower <= upper where both have values
            both_have_values = lower_has_value & upper_has_value
            if both_have_values.any():
                invalid_ranges = df.loc[both_have_values, lower_col] > df.loc[both_have_values, upper_col]
                if invalid_ranges.any():
                    invalid_rows = df[both_have_values][invalid_ranges].index.tolist()
                    raise EpidemiologyValidationError(
                        f"Lower bound must be <= upper bound for {lower_col}/{upper_col}. "
                        f"Invalid rows: {invalid_rows}"
                    )
        elif lower_col in df.columns or upper_col in df.columns:
            # Only one present - this is an error
            present_col = lower_col if lower_col in df.columns else upper_col
            missing_col = upper_col if lower_col in df.columns else lower_col
            raise EpidemiologyValidationError(
                f"Paired field {missing_col} is missing when {present_col} is present"
            )

    return df


class BasicEpidemiologySchema:
    """Basic epidemiology data schema using Pandera"""

    schema = basic_epidemiology_schema

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validate DataFrame against basic epidemiology schema"""
        try:
            return cls.schema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            logger = get_dagster_logger()
            logger.error(f"Basic epidemiology schema validation failed: {e}")
            raise EpidemiologyValidationError(f"Validation failed: {e}")

    @classmethod
    def transform_from_source(cls, df: pd.DataFrame, jurisdiction: str = "Unknown") -> pd.DataFrame:
        """
        Transform source data to basic epidemiology schema format.

        Expects input DataFrame with 'Date' and 'Count' columns.
        """
        if df.empty:
            return pd.DataFrame(columns=cls.schema.columns.keys())

        # Validate input
        required_input_cols = ['Date', 'Count']
        missing_cols = set(required_input_cols) - set(df.columns)
        if missing_cols:
            raise EpidemiologyValidationError(
                f"Input DataFrame missing required columns: {missing_cols}"
            )

        # Transform
        df = df.copy()
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')

        # Remove rows with invalid dates
        df = df.dropna(subset=['Date'])

        if df.empty:
            return pd.DataFrame(columns=cls.schema.columns.keys())

        transformed_df = pd.DataFrame()

        # Date columns (create rows first)
        transformed_df['date_week_start'] = df['Date'].dt.strftime('%Y-%m-%d')
        transformed_df['date_week_end'] = (df['Date'] + pd.Timedelta(days=7)).dt.strftime('%Y-%m-%d')

        # Week and year calculations
        iso_calendar = df['Date'].dt.isocalendar()
        transformed_df['Week_Number'] = iso_calendar['week'].astype(int)
        transformed_df['Year'] = iso_calendar['year'].astype(int)
        transformed_df['Week_Year'] = (
            transformed_df['Week_Number'].astype(str) + '-' +
            transformed_df['Year'].astype(str)
        )

        # Cases
        transformed_df['Cases'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0).astype(int)

        # Jurisdiction (set after rows exist, ensure no spaces)
        transformed_df['Jurisdiction'] = jurisdiction.replace(' ', '')

        # Reorder columns to match specification: Jurisdiction, date_week_start, date_week_end, Week_Number, Year, Week_Year, Cases
        column_order = ['Jurisdiction', 'date_week_start', 'date_week_end', 'Week_Number', 'Year', 'Week_Year', 'Cases']
        transformed_df = transformed_df[column_order]

        return cls.validate(transformed_df)


class StatisticalExtensionSchema:
    """Statistical extension epidemiology data schema using Pandera"""

    base_schema = statistical_extension_schema_base

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validate DataFrame against Statistical Extension schema"""
        try:
            # First validate required columns with base schema
            validated_df = cls.base_schema.validate(df, lazy=True)

            # Then validate any optional columns that are present
            for col_name, col_schema in OPTIONAL_COLUMNS.items():
                if col_name in df.columns:
                    # Create a temporary schema just for this column
                    temp_schema = DataFrameSchema({col_name: col_schema}, strict=False)
                    df_subset = df[[col_name]]
                    temp_schema.validate(df_subset, lazy=True)

            # Finally validate business rules
            return validate_statistical_extension_requirements(validated_df)
        except pa.errors.SchemaErrors as e:
            logger = get_dagster_logger()
            logger.error(f"Statistical Extension schema validation failed: {e}")
            raise EpidemiologyValidationError(f"Validation failed: {e}")

    @classmethod
    def create_record(cls,
                     jurisdiction: str,
                     date: Union[str, datetime],
                     disease: str,
                     metric: str,
                     observation_type: str,
                     **optional_fields) -> pd.DataFrame:
        """Create a single record following the Statistical Extension schema"""

        # Convert date to string format
        if isinstance(date, datetime):
            date_str = date.strftime('%Y-%m-%d')
        else:
            date_str = str(date)

        # Build record
        record = {
            'Jurisdiction': jurisdiction,
            'date': date_str,
            'disease': disease,
            'metric': metric,
            'observation_type': observation_type
        }

        # Add optional fields
        record.update(optional_fields)

        df = pd.DataFrame([record])
        return cls.validate(df)


class ResilientEpiProcessor:
    """
    Main processor for epidemiology data using Resilient Epi Schemas.

    Handles validation and output for both basic epidemiology and statistical extension schemas.
    """

    def __init__(self):
        self.logger = get_dagster_logger()

    def process_basic_epidemiology_data(self,
                                       df: pd.DataFrame,
                                       jurisdiction: str = "Unknown",
                                       validate: bool = True) -> pd.DataFrame:
        """Process data using basic epidemiology schema"""
        try:
            # Transform to schema format
            transformed_df = BasicEpidemiologySchema.transform_from_source(df, jurisdiction)

            # Validation is done in transform_from_source - no need to log each success
            return transformed_df

        except Exception as e:
            self.logger.error(f"Error processing basic epidemiology data: {e}")
            raise

    def process_statistical_extension_data(self,
                                         df: pd.DataFrame,
                                         validate: bool = True) -> pd.DataFrame:
        """Process data using statistical extension schema"""
        try:
            if validate:
                validated_df = StatisticalExtensionSchema.validate(df)
                return validated_df
            else:
                return df.copy()

        except Exception as e:
            self.logger.error(f"Error processing statistical extension data: {e}")
            raise

    def write_output(self,
                     df: pd.DataFrame,
                     s3_resource,
                     s3_path: str,
                     metadata: Optional[Dict[str, Any]] = None,
                     formats: List[str] = ['csv', 'json']) -> None:
        """Write validated epidemiology data to S3 storage"""
        try:
            from . import store_assets

            # Try to create GeoDataFrame if possible
            try:
                import geopandas as gpd
                gdf = gpd.GeoDataFrame(df)
                store_assets.geodataframe_to_s3(gdf, s3_path, s3_resource,
                                              metadata=metadata, formats=formats)
                self.logger.info(f"Stored epidemiology data as GeoDataFrame to S3: {s3_path}")
            except Exception as geo_error:
                self.logger.warning(f"Could not create GeoDataFrame: {geo_error}. Using DataFrame.")
                store_assets.dataframe_to_s3(df, s3_path, s3_resource,
                                           metadata=metadata, formats=formats)
                self.logger.info(f"Stored epidemiology data as DataFrame to S3: {s3_path}")

        except Exception as e:
            self.logger.error(f"Error writing epidemiology data output: {e}")
            raise


# Convenience functions for asset use
def validate_basic_epidemiology_format(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and return DataFrame in basic epidemiology format"""
    return BasicEpidemiologySchema.validate(df)


def validate_statistical_extension_format(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and return DataFrame in statistical extension format"""
    return StatisticalExtensionSchema.validate(df)


def transform_to_basic_epidemiology(df: pd.DataFrame, jurisdiction: str = "Unknown") -> pd.DataFrame:
    """Transform source data to basic epidemiology format"""
    return BasicEpidemiologySchema.transform_from_source(df, jurisdiction)


def create_statistical_extension_record(jurisdiction: str,
                                       date: Union[str, datetime],
                                       disease: str,
                                       metric: str,
                                       observation_type: str,
                                       **optional_fields) -> pd.DataFrame:
    """Create a statistical extension format record"""
    return StatisticalExtensionSchema.create_record(
        jurisdiction, date, disease, metric, observation_type, **optional_fields
    )