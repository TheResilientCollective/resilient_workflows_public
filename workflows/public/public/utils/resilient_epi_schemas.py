"""
Resilient Epidemiology Data Schemas

This module provides Pandera-based data schemas and validation for epidemiology data output,
supporting both basic epidemiology format and statistical extension format
as specified in the epidemiology data plan.
"""
import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
from dagster import get_dagster_logger
import re
from epiweeks import Week, Year


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
        description="Epi week number (1-53)"
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
    # Normal, Cases_Removed, Correction, Original
    "Week_Type": Column(
        str,
        default='Original',
        checks=[
            Check(lambda s: s.str.len() > 0,
                  error="Week_Type cannot be empty")
        ],
        description="Normal if current_week and cummulative YTD calculation matched. Adjustment if corrected, Adjustment_Cases_Removed if negative"
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
        transformed_df['date_week_end'] = (df['Date'] + pd.Timedelta(days=6)).dt.strftime('%Y-%m-%d')

        # Week and year calculations
        #iso_calendar = df['Date'].dt.isocalendar()

        #transformed_df['Week_Number'] = iso_calendar['week'].astype(int)
        #transformed_df['Year'] = iso_calendar['year'].astype(int)
        transformed_df['Week_Number'] =  df['Date'].apply(lambda d : Week.fromdate(d, system='cdc').week)

        transformed_df['Year'] =  df['Date'].apply(lambda d : Week.fromdate(d, system='cdc').year)
        transformed_df['Week_Year'] = (
            transformed_df['Week_Number'].astype(str) + '-' +
            transformed_df['Year'].astype(str)
        )
        if 'Week_Type' in df.columns:
            transformed_df['Week_Type'] = df['Week_Type'].fillna('Original')
        else:
            transformed_df['Week_Type'] = 'Original'
        # Cases
        transformed_df['Cases'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0).astype(int)

        # Jurisdiction (set after rows exist, ensure no spaces)
        transformed_df['Jurisdiction'] = jurisdiction.replace(' ', '')

        # Reorder columns to match specification: Jurisdiction, date_week_start, date_week_end, Week_Number, Year, Week_Year, Cases
        column_order = ['Jurisdiction', 'date_week_start', 'date_week_end', 'Week_Number', 'Year', 'Week_Year', 'Cases', 'Week_Type']
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


def detailed_epidemiology_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform WAHIS parquet data to detailed epidemiology format.

    This function processes WAHIS (World Animal Health Information System) data
    to create a detailed epidemiology dataset with proper time periods and geography.

    Args:
        df: DataFrame with WAHIS data containing columns like Report_id,
            Outbreak_start_date, Location_name, disease_eng, etc.

    Returns:
        DataFrame in detailed epidemiology format with columns:
        - Report_id: Links related outbreak reports
        - date_period_start: Start date of the outbreak period (Outbreak_start_date)
        - date_period_end: End date of period (next report's start or outbreak end)
        - jurisdiction: Location name
        - disease_eng: Disease in English
        - country, region: Geographic identifiers
        - Location_aprox: Boolean indicating if location is approximate
        - reason_of_notification: Categorical notification reason
        - Species, quantitative_unit: Categorical fields
        - cases, susceptible, dead, killed_disposed, slaughtered, vaccinated,
          morbidity, mortality: Numerical outbreak data
        - Longitude, Latitude: Decimal degree coordinates
        - geometry: POINT geometry from coordinates
        - Week_Number, Year, Week_Year: Time-based fields from outbreak start date
    """
    import geopandas as gpd
    from shapely.geometry import Point

    logger = get_dagster_logger()

    if df.empty:
        logger.warning("Empty DataFrame provided to detailed_epidemiology_format")
        return pd.DataFrame()

    # Validate required columns
    required_cols = ['Report_id', 'Outbreak_start_date', 'Location_name', 'disease_eng']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise EpidemiologyValidationError(
            f"Missing required columns for WAHIS format: {missing_cols}"
        )

    # Sort by Report_id then Outbreak_start_date as specified
    df_sorted = df.copy().sort_values(['Report_id', 'Outbreak_start_date'])

    # Initialize result DataFrame
    result_df = pd.DataFrame()

    # Core identification fields
    result_df['Report_id'] = df_sorted['Report_id']
    result_df['date_period_start'] = df_sorted['Outbreak_start_date'].dt.strftime('%Y-%m-%d')

    # Calculate date_period_end: next report's start date or outbreak end date
    result_df['date_period_end'] = ''
    for report_id in df_sorted['Report_id'].unique():
        mask = df_sorted['Report_id'] == report_id
        report_data = df_sorted[mask].copy()

        if len(report_data) > 1:
            # Multiple reports: end date is next report's start date
            for i in range(len(report_data)):
                idx = report_data.index[i]
                if i < len(report_data) - 1:
                    # Not the last report - use next report's start date
                    next_start = report_data.iloc[i + 1]['Outbreak_start_date']
                    result_df.loc[df_sorted.index == idx, 'date_period_end'] = next_start.strftime('%Y-%m-%d')
                else:
                    # Last report - use outbreak end date
                    end_date = report_data.iloc[i]['Outbreak_end_date']
                    if pd.notna(end_date):
                        result_df.loc[df_sorted.index == idx, 'date_period_end'] = end_date.strftime('%Y-%m-%d')
                    else:
                        # If no end date, use start date (single day outbreak)
                        result_df.loc[df_sorted.index == idx, 'date_period_end'] = report_data.iloc[i]['Outbreak_start_date'].strftime('%Y-%m-%d')
        else:
            # Single report: use outbreak end date
            idx = report_data.index[0]
            end_date = report_data.iloc[0]['Outbreak_end_date']
            if pd.notna(end_date):
                result_df.loc[df_sorted.index == idx, 'date_period_end'] = end_date.strftime('%Y-%m-%d')
            else:
                # If no end date, use start date
                result_df.loc[df_sorted.index == idx, 'date_period_end'] = report_data.iloc[0]['Outbreak_start_date'].strftime('%Y-%m-%d')

    # Geographic and disease information
    result_df['jurisdiction'] = df_sorted['Location_name']
    result_df['disease_eng'] = df_sorted['disease_eng']

    # Geographic fields (with default empty values for missing columns)
    result_df['country'] = df_sorted.get('country', '')
    result_df['region'] = df_sorted.get('region', '')

    # Location approximation (convert to boolean)
    if 'Location_aprox' in df_sorted.columns:
        result_df['Location_aprox'] = df_sorted['Location_aprox'].astype(bool)
    else:
        result_df['Location_aprox'] = False

    # Categorical fields
    result_df['reason_of_notification'] = df_sorted.get('reason of notification', '')
    result_df['Species'] = df_sorted.get('Species', '')
    result_df['quantitative_unit'] = df_sorted.get('quantitative_unit', '')

    # Numerical outbreak data columns
    numerical_cols = ['cases', 'susceptible', 'dead', 'killed_disposed',
                     'slaughtered', 'vaccinated', 'morbidity', 'mortality']
    for col in numerical_cols:
        if col in df_sorted.columns:
            result_df[col] = pd.to_numeric(df_sorted[col], errors='coerce').fillna(0).astype(int)
        else:
            result_df[col] = 0

    # Coordinate fields
    result_df['Longitude'] = pd.to_numeric(df_sorted.get('Longitude', np.nan), errors='coerce')
    result_df['Latitude'] = pd.to_numeric(df_sorted.get('Latitude', np.nan), errors='coerce')

    # Create geometry column from coordinates
    def create_point(row):
        """Create Point geometry from lon/lat if both are valid"""
        try:
            if pd.notna(row['Longitude']) and pd.notna(row['Latitude']):
                return Point(row['Longitude'], row['Latitude'])
            return None
        except Exception:
            return None

    result_df['geometry'] = result_df.apply(create_point, axis=1)

    # Time-based fields from Outbreak_start_date
    outbreak_dates = pd.to_datetime(df_sorted['Outbreak_start_date'])
    iso_calendar = outbreak_dates.dt.isocalendar()

    result_df['Week_Number'] = iso_calendar['week'].astype(int)
    result_df['Year'] = iso_calendar['year'].astype(int)
    result_df['Week_Year'] = (
        result_df['Week_Number'].astype(str) + '-' +
        result_df['Year'].astype(str)
    )

    # Convert to GeoDataFrame if geometry column has valid points
    has_valid_geometry = result_df['geometry'].notna().any()
    if has_valid_geometry:
        try:
            result_gdf = gpd.GeoDataFrame(result_df, geometry='geometry', crs='EPSG:4326')
            logger.info(f"Created GeoDataFrame with {has_valid_geometry} valid geometries")
            return result_gdf
        except Exception as e:
            logger.warning(f"Failed to create GeoDataFrame: {e}. Returning regular DataFrame.")

    logger.info(f"Processed {len(result_df)} WAHIS records into detailed epidemiology format")
    return result_df
