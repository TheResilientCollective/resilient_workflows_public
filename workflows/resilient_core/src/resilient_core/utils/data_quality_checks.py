"""
Data Quality Checks for Epidemiological Surveillance Data

This module provides asset checks for validating data completeness and quality
in epidemiological surveillance datasets, with focus on temporal completeness
and disease-specific validation.
"""

import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Union
from dagster import get_dagster_logger, asset_check, AssetCheckResult, AssetCheckSeverity
import numpy as np


class DataCompletenessError(Exception):
    """Custom exception for data completeness validation errors"""
    pass


def generate_expected_weekly_dates(start_date: Union[str, datetime, date],
                                 end_date: Union[str, datetime, date]) -> List[str]:
    """
    Generate a list of expected weekly dates (Monday start) between start_date and end_date.

    Args:
        start_date: Start date for the range
        end_date: End date for the range

    Returns:
        List of date strings in YYYY-MM-DD format
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    elif isinstance(start_date, datetime):
        start_date = start_date.date()

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    elif isinstance(end_date, datetime):
        end_date = end_date.date()

    # Find the Monday of the week containing start_date
    days_since_monday = start_date.weekday()
    week_start = start_date - timedelta(days=days_since_monday)

    weekly_dates = []
    current_date = week_start

    while current_date <= end_date:
        weekly_dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=7)

    return weekly_dates


def check_weekly_completeness(df: pd.DataFrame,
                            date_column: str = 'date_week_start',
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None,
                            group_by: Optional[List[str]] = None) -> Dict:
    """
    Check for missing weeks in epidemiological data.

    Args:
        df: DataFrame with epidemiological data
        date_column: Name of the date column to check
        start_date: Start date for completeness check (default: min date in data)
        end_date: End date for completeness check (default: max date in data)
        group_by: List of columns to group by (e.g., ['Jurisdiction', 'disease'])

    Returns:
        Dictionary with completeness results
    """
    if df.empty:
        return {
            'status': 'FAIL',
            'message': 'DataFrame is empty',
            'missing_weeks': [],
            'completeness_pct': 0.0,
            'total_expected': 0,
            'total_found': 0
        }

    if date_column not in df.columns:
        return {
            'status': 'FAIL',
            'message': f'Date column "{date_column}" not found in DataFrame',
            'missing_weeks': [],
            'completeness_pct': 0.0,
            'total_expected': 0,
            'total_found': 0
        }

    # Determine date range
    if start_date is None:
        start_date = df[date_column].min()
    if end_date is None:
        end_date = df[date_column].max()

    # Generate expected weekly dates
    expected_dates = generate_expected_weekly_dates(start_date, end_date)
    expected_dates_set = set(expected_dates)

    if group_by:
        # Check completeness by group
        results = {}
        overall_missing = []
        overall_expected_total = 0
        overall_found_total = 0

        for group_values, group_df in df.groupby(group_by):
            group_key = group_values if isinstance(group_values, tuple) else (group_values,)
            group_name = ' | '.join(str(v) for v in group_key)

            actual_dates_set = set(group_df[date_column].astype(str))
            missing_dates = sorted(expected_dates_set - actual_dates_set)

            completeness_pct = ((len(expected_dates) - len(missing_dates)) / len(expected_dates)) * 100

            results[group_name] = {
                'missing_weeks': missing_dates,
                'completeness_pct': completeness_pct,
                'total_expected': len(expected_dates),
                'total_found': len(actual_dates_set),
                'status': 'PASS' if len(missing_dates) == 0 else 'FAIL'
            }

            overall_missing.extend([f"{group_name}: {date}" for date in missing_dates])
            overall_expected_total += len(expected_dates)
            overall_found_total += len(actual_dates_set)

        overall_completeness = (overall_found_total / overall_expected_total) * 100 if overall_expected_total > 0 else 0

        return {
            'status': 'PASS' if len(overall_missing) == 0 else 'FAIL',
            'message': f'Checked {len(results)} groups',
            'missing_weeks': overall_missing,
            'completeness_pct': overall_completeness,
            'total_expected': overall_expected_total,
            'total_found': overall_found_total,
            'group_results': results
        }
    else:
        # Check overall completeness
        actual_dates_set = set(df[date_column].astype(str))
        missing_dates = sorted(expected_dates_set - actual_dates_set)

        completeness_pct = ((len(expected_dates) - len(missing_dates)) / len(expected_dates)) * 100

        return {
            'status': 'PASS' if len(missing_dates) == 0 else 'FAIL',
            'message': f'Overall completeness check',
            'missing_weeks': missing_dates,
            'completeness_pct': completeness_pct,
            'total_expected': len(expected_dates),
            'total_found': len(actual_dates_set)
        }


def check_disease_completeness(df: pd.DataFrame,
                             disease_column: str = 'disease',
                             date_column: str = 'date_week_start',
                             jurisdiction_column: str = 'Jurisdiction',
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> Dict:
    """
    Check completeness across diseases and jurisdictions.

    Args:
        df: DataFrame with epidemiological data
        disease_column: Name of the disease column
        date_column: Name of the date column
        jurisdiction_column: Name of the jurisdiction column
        start_date: Start date for completeness check
        end_date: End date for completeness check

    Returns:
        Dictionary with disease-specific completeness results
    """
    if df.empty:
        return {
            'status': 'FAIL',
            'message': 'DataFrame is empty',
            'disease_results': {}
        }

    diseases = df[disease_column].unique()
    jurisdictions = df[jurisdiction_column].unique()

    disease_results = {}
    overall_status = 'PASS'

    for disease in diseases:
        disease_df = df[df[disease_column] == disease]

        # Check completeness for this disease across all jurisdictions
        completeness_result = check_weekly_completeness(
            disease_df,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date,
            group_by=[jurisdiction_column]
        )

        disease_results[disease] = completeness_result

        if completeness_result['status'] == 'FAIL':
            overall_status = 'FAIL'

    return {
        'status': overall_status,
        'message': f'Checked completeness for {len(diseases)} diseases across {len(jurisdictions)} jurisdictions',
        'disease_results': disease_results,
        'total_diseases': len(diseases),
        'total_jurisdictions': len(jurisdictions)
    }


def format_completeness_message(results: Dict, asset_name: str) -> str:
    """
    Format completeness check results into a readable message.

    Args:
        results: Results dictionary from completeness check
        asset_name: Name of the asset being checked

    Returns:
        Formatted message string
    """
    if results['status'] == 'PASS':
        if 'group_results' in results:
            group_count = len(results['group_results'])
            return f"✅ {asset_name}: Complete weekly data for all {group_count} groups. {results['completeness_pct']:.1f}% complete."
        elif 'disease_results' in results:
            disease_count = results['total_diseases']
            jurisdiction_count = results['total_jurisdictions']
            return f"✅ {asset_name}: Complete weekly data for {disease_count} diseases across {jurisdiction_count} jurisdictions."
        else:
            return f"✅ {asset_name}: Complete weekly data. {results['completeness_pct']:.1f}% complete."

    else:
        missing_count = len(results.get('missing_weeks', []))
        completeness = results.get('completeness_pct', 0)

        if 'group_results' in results:
            # Group-based results
            group_count = len(results['group_results'])
            failing_groups = [name for name, res in results['group_results'].items() if res['status'] == 'FAIL']

            message = f"❌ {asset_name}: Missing weekly data. {completeness:.1f}% complete.\n"
            message += f"   • {len(failing_groups)}/{group_count} groups have missing weeks\n"

            if missing_count <= 10:
                message += f"   • Missing: {', '.join(results['missing_weeks'][:10])}"
            else:
                message += f"   • Missing: {missing_count} weeks (showing first 10: {', '.join(results['missing_weeks'][:10])}...)"

        elif 'disease_results' in results:
            # Disease-based results
            disease_count = results['total_diseases']
            failing_diseases = [disease for disease, res in results['disease_results'].items() if res['status'] == 'FAIL']

            message = f"❌ {asset_name}: Missing weekly data for diseases.\n"
            message += f"   • {len(failing_diseases)}/{disease_count} diseases have missing weeks\n"
            message += f"   • Failing diseases: {', '.join(failing_diseases)}"

        else:
            # Simple results
            message = f"❌ {asset_name}: Missing {missing_count} weeks. {completeness:.1f}% complete.\n"

            if missing_count <= 10:
                message += f"   • Missing: {', '.join(results['missing_weeks'])}"
            else:
                message += f"   • Missing: {', '.join(results['missing_weeks'][:10])}... (and {missing_count-10} more)"

        return message


def load_resilient_epi_data(s3_resource, s3_path: str) -> pd.DataFrame:
    """
    Load resilient epi schema data from S3 for quality checking.

    Args:
        s3_resource: Dagster S3 resource
        s3_path: S3 path to the data file (without extension)

    Returns:
        DataFrame with the loaded data
    """
    try:
        # Try to load as CSV first
        csv_path = f"{s3_path}.csv"
        df = pd.read_csv(f"s3://{s3_resource.bucket}/{csv_path}")
        return df
    except Exception as csv_error:
        try:
            # Try to load as GeoJSON
            geojson_path = f"{s3_path}.geojson"
            gdf = gpd.read_file(f"s3://{s3_resource.bucket}/{geojson_path}")
            return pd.DataFrame(gdf.drop(columns=['geometry'], errors='ignore'))
        except Exception as geojson_error:
            raise DataCompletenessError(
                f"Could not load data from {s3_path}. CSV error: {csv_error}. GeoJSON error: {geojson_error}"
            )