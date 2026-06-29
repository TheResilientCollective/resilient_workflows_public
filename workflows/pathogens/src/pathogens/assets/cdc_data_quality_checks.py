"""
CDC Surveillance Data Quality Checks

Asset checks for validating data completeness and quality in CDC surveillance datasets.
Ensures weekly data completeness across jurisdictions and diseases.
"""

import pandas as pd
from datetime import datetime, timedelta
from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckSeverity,
    get_dagster_logger,
    asset,
    AssetKey
)
from resilient_core.utils.data_quality_checks import (
    check_weekly_completeness,
    check_disease_completeness,
    format_completeness_message,
    load_resilient_epi_data,
    DataCompletenessError
)


# Configuration for surveillance period
SURVEILLANCE_START_DATE = "2022-01-01"  # Start of consistent surveillance
SURVEILLANCE_END_DATE = None  # None = current date


@asset_check(asset=AssetKey(["cdc", "mpox_weekly"]), name="mpox_weekly_completeness",
             required_resource_keys={'s3'})
def check_mpox_weekly_completeness(context) -> AssetCheckResult:
    """
    Check that mpox weekly basic epidemiology data has no missing weeks by jurisdiction.

    Validates that each jurisdiction has complete weekly surveillance data with no gaps.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the basic epidemiology schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/mpox_weekly_basic'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Mpox basic epidemiology data is empty"
            )

        logger.info(f"🔍 Checking weekly completeness for {len(df)} mpox records")

        # Check completeness by jurisdiction
        results = check_weekly_completeness(
            df,
            date_column='date_week_start',
            start_date=SURVEILLANCE_START_DATE,
            end_date=SURVEILLANCE_END_DATE,
            group_by=['Jurisdiction']
        )

        # Format message
        message = format_completeness_message(results, "Mpox Weekly Data")

        # Additional details for metadata
        metadata = {
            'total_records': len(df),
            'jurisdictions_checked': len(results.get('group_results', {})),
            'completeness_percentage': results.get('completeness_pct', 0),
            'missing_weeks_count': len(results.get('missing_weeks', [])),
            'check_period_start': SURVEILLANCE_START_DATE,
            'check_period_end': SURVEILLANCE_END_DATE or datetime.now().strftime('%Y-%m-%d')
        }

        return AssetCheckResult(
            passed=(results['status'] == 'PASS'),
            severity=AssetCheckSeverity.WARN if results['status'] == 'FAIL' else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except DataCompletenessError as e:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Data loading error: {e}"
        )
    except Exception as e:
        logger.error(f"Error checking mpox weekly completeness: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during completeness check: {e}"
        )


@asset_check(asset=AssetKey(["cdc", "measles_weekly"]), name="measles_weekly_completeness",
             required_resource_keys={'s3'})
def check_measles_weekly_completeness(context) -> AssetCheckResult:
    """
    Check that measles weekly basic epidemiology data has no missing weeks by jurisdiction.

    Validates that each jurisdiction has complete weekly surveillance data with no gaps.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the basic epidemiology schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/measles_weekly_basic'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Measles basic epidemiology data is empty"
            )

        logger.info(f"🔍 Checking weekly completeness for {len(df)} measles records")

        # Check completeness by jurisdiction and disease type
        results = check_weekly_completeness(
            df,
            date_column='date_week_start',
            start_date=SURVEILLANCE_START_DATE,
            end_date=SURVEILLANCE_END_DATE,
            group_by=['Jurisdiction', 'disease']
        )

        # Format message
        message = format_completeness_message(results, "Measles Weekly Data")

        # Additional details for metadata
        metadata = {
            'total_records': len(df),
            'jurisdiction_disease_combinations': len(results.get('group_results', {})),
            'completeness_percentage': results.get('completeness_pct', 0),
            'missing_weeks_count': len(results.get('missing_weeks', [])),
            'check_period_start': SURVEILLANCE_START_DATE,
            'check_period_end': SURVEILLANCE_END_DATE or datetime.now().strftime('%Y-%m-%d'),
            'disease_types': df['disease'].unique().tolist() if 'disease' in df.columns else []
        }

        return AssetCheckResult(
            passed=(results['status'] == 'PASS'),
            severity=AssetCheckSeverity.WARN if results['status'] == 'FAIL' else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except DataCompletenessError as e:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Data loading error: {e}"
        )
    except Exception as e:
        logger.error(f"Error checking measles weekly completeness: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during completeness check: {e}"
        )


@asset_check(asset=AssetKey(["cdc", "mpox_weekly"]), name="mpox_statistical_extension_completeness",
             required_resource_keys={'s3'})
def check_mpox_statistical_extension_completeness(context) -> AssetCheckResult:
    """
    Check that mpox statistical extension data has complete weekly metrics by jurisdiction.

    Validates statistical extension format data for metric completeness and temporal coverage.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the statistical extension schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/mpox_weekly_statistical'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Mpox statistical extension data is empty"
            )

        logger.info(f"🔍 Checking statistical extension completeness for {len(df)} mpox records")

        # Check completeness by jurisdiction and metric type
        results = check_weekly_completeness(
            df,
            date_column='date',  # Statistical extension uses 'date' not 'date_week_start'
            start_date=SURVEILLANCE_START_DATE,
            end_date=SURVEILLANCE_END_DATE,
            group_by=['Jurisdiction', 'metric']
        )

        # Additional check: ensure all expected metrics are present
        expected_metrics = ['cases']  # Could extend to include deaths, hospitalizations, etc.
        actual_metrics = df['metric'].unique().tolist() if 'metric' in df.columns else []
        missing_metrics = set(expected_metrics) - set(actual_metrics)

        # Format message
        message = format_completeness_message(results, "Mpox Statistical Extension Data")

        if missing_metrics:
            message += f"\n   • Missing metric types: {', '.join(missing_metrics)}"
            results['status'] = 'FAIL'

        # Additional details for metadata
        metadata = {
            'total_records': len(df),
            'jurisdiction_metric_combinations': len(results.get('group_results', {})),
            'completeness_percentage': results.get('completeness_pct', 0),
            'missing_weeks_count': len(results.get('missing_weeks', [])),
            'metrics_present': actual_metrics,
            'metrics_missing': list(missing_metrics),
            'check_period_start': SURVEILLANCE_START_DATE,
            'check_period_end': SURVEILLANCE_END_DATE or datetime.now().strftime('%Y-%m-%d')
        }

        return AssetCheckResult(
            passed=(results['status'] == 'PASS' and len(missing_metrics) == 0),
            severity=AssetCheckSeverity.WARN if results['status'] == 'FAIL' or missing_metrics else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except DataCompletenessError as e:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Data loading error: {e}"
        )
    except Exception as e:
        logger.error(f"Error checking mpox statistical extension completeness: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during completeness check: {e}"
        )


@asset_check(asset=AssetKey(["cdc", "measles_weekly"]), name="measles_statistical_extension_completeness",
             required_resource_keys={'s3'})
def check_measles_statistical_extension_completeness(context) -> AssetCheckResult:
    """
    Check that measles statistical extension data has complete weekly metrics by jurisdiction.

    Validates statistical extension format data for metric completeness and temporal coverage.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the statistical extension schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/measles_weekly_statistical'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Measles statistical extension data is empty"
            )

        logger.info(f"🔍 Checking statistical extension completeness for {len(df)} measles records")

        # Check completeness by jurisdiction, disease type, and metric
        results = check_weekly_completeness(
            df,
            date_column='date',  # Statistical extension uses 'date' not 'date_week_start'
            start_date=SURVEILLANCE_START_DATE,
            end_date=SURVEILLANCE_END_DATE,
            group_by=['Jurisdiction', 'disease', 'metric']
        )

        # Additional check: ensure all expected metrics are present per disease type
        expected_metrics = ['cases']
        disease_types = df['disease'].unique() if 'disease' in df.columns else []

        missing_combinations = []
        for disease in disease_types:
            disease_df = df[df['disease'] == disease]
            actual_metrics = disease_df['metric'].unique().tolist() if 'metric' in disease_df.columns else []
            missing_for_disease = set(expected_metrics) - set(actual_metrics)
            if missing_for_disease:
                missing_combinations.extend([f"{disease}:{metric}" for metric in missing_for_disease])

        # Format message
        message = format_completeness_message(results, "Measles Statistical Extension Data")

        if missing_combinations:
            message += f"\n   • Missing disease:metric combinations: {', '.join(missing_combinations)}"
            results['status'] = 'FAIL'

        # Additional details for metadata
        metadata = {
            'total_records': len(df),
            'jurisdiction_disease_metric_combinations': len(results.get('group_results', {})),
            'completeness_percentage': results.get('completeness_pct', 0),
            'missing_weeks_count': len(results.get('missing_weeks', [])),
            'disease_types': list(disease_types),
            'missing_disease_metric_combinations': missing_combinations,
            'check_period_start': SURVEILLANCE_START_DATE,
            'check_period_end': SURVEILLANCE_END_DATE or datetime.now().strftime('%Y-%m-%d')
        }

        return AssetCheckResult(
            passed=(results['status'] == 'PASS' and len(missing_combinations) == 0),
            severity=AssetCheckSeverity.WARN if results['status'] == 'FAIL' or missing_combinations else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except DataCompletenessError as e:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Data loading error: {e}"
        )
    except Exception as e:
        logger.error(f"Error checking measles statistical extension completeness: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during completeness check: {e}"
        )


@asset_check(asset=AssetKey(["cdc", "mpox_weekly"]), name="mpox_zero_counts_validation",
             required_resource_keys={'s3'})
def check_mpox_zero_counts_preserved(context) -> AssetCheckResult:
    """
    Validate that zero counts are properly preserved in mpox surveillance data.

    Ensures that weeks with zero cases are included in the dataset (not filtered out).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the basic epidemiology schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/mpox_weekly_basic'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Mpox basic epidemiology data is empty"
            )

        logger.info(f"🔍 Validating zero count preservation in {len(df)} mpox records")

        # Check for zero counts
        zero_count_records = df[df['Cases'] == 0] if 'Cases' in df.columns else pd.DataFrame()
        total_records = len(df)
        zero_count_records_count = len(zero_count_records)
        zero_percentage = (zero_count_records_count / total_records) * 100 if total_records > 0 else 0

        # Check distribution by jurisdiction
        jurisdiction_stats = {}
        if 'Jurisdiction' in df.columns:
            for jurisdiction in df['Jurisdiction'].unique():
                jurisdiction_df = df[df['Jurisdiction'] == jurisdiction]
                jurisdiction_zeros = len(jurisdiction_df[jurisdiction_df['Cases'] == 0])
                jurisdiction_total = len(jurisdiction_df)
                jurisdiction_stats[jurisdiction] = {
                    'total_records': jurisdiction_total,
                    'zero_records': jurisdiction_zeros,
                    'zero_percentage': (jurisdiction_zeros / jurisdiction_total) * 100 if jurisdiction_total > 0 else 0
                }

        # Determine if zero counts are appropriately preserved
        # We expect some zero counts in surveillance data (disease-free periods)
        has_zero_counts = zero_count_records_count > 0
        reasonable_zero_rate = 0 <= zero_percentage <= 95  # Between 0% and 95% zeros is reasonable

        passed = has_zero_counts and reasonable_zero_rate

        if passed:
            message = f"✅ Mpox zero counts preserved: {zero_count_records_count}/{total_records} records ({zero_percentage:.1f}%) have zero cases"
        else:
            if not has_zero_counts:
                message = f"⚠️  Mpox: No zero-count records found. This may indicate zero counts are being filtered out."
            else:
                message = f"⚠️  Mpox: Unusual zero count rate: {zero_percentage:.1f}% of records have zero cases"

        metadata = {
            'total_records': total_records,
            'zero_count_records': zero_count_records_count,
            'zero_percentage': zero_percentage,
            'jurisdictions_with_zeros': len([j for j, stats in jurisdiction_stats.items() if stats['zero_records'] > 0]),
            'jurisdiction_stats': jurisdiction_stats
        }

        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except Exception as e:
        logger.error(f"Error checking mpox zero counts: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during zero counts validation: {e}"
        )


@asset_check(asset=AssetKey(["cdc", "measles_weekly"]), name="measles_zero_counts_validation",
             required_resource_keys={'s3'})
def check_measles_zero_counts_preserved(context) -> AssetCheckResult:
    """
    Validate that zero counts are properly preserved in measles surveillance data.

    Ensures that weeks with zero cases are included in the dataset (not filtered out).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # Load the basic epidemiology schema data
        s3_path = 'pathogens/cdc/nndss/output/validated_epi_schema/measles_weekly_basic'
        df = load_resilient_epi_data(s3_resource, s3_path)

        if df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Measles basic epidemiology data is empty"
            )

        logger.info(f"🔍 Validating zero count preservation in {len(df)} measles records")

        # Check for zero counts
        zero_count_records = df[df['Cases'] == 0] if 'Cases' in df.columns else pd.DataFrame()
        total_records = len(df)
        zero_count_records_count = len(zero_count_records)
        zero_percentage = (zero_count_records_count / total_records) * 100 if total_records > 0 else 0

        # Check distribution by jurisdiction and disease type
        group_stats = {}
        if 'Jurisdiction' in df.columns and 'disease' in df.columns:
            for (jurisdiction, disease), group_df in df.groupby(['Jurisdiction', 'disease']):
                group_zeros = len(group_df[group_df['Cases'] == 0])
                group_total = len(group_df)
                group_stats[f"{jurisdiction}:{disease}"] = {
                    'total_records': group_total,
                    'zero_records': group_zeros,
                    'zero_percentage': (group_zeros / group_total) * 100 if group_total > 0 else 0
                }

        # Determine if zero counts are appropriately preserved
        # For measles, we expect many zero counts (disease is relatively rare)
        has_zero_counts = zero_count_records_count > 0
        reasonable_zero_rate = 0 <= zero_percentage <= 98  # Up to 98% zeros is reasonable for measles

        passed = has_zero_counts and reasonable_zero_rate

        if passed:
            message = f"✅ Measles zero counts preserved: {zero_count_records_count}/{total_records} records ({zero_percentage:.1f}%) have zero cases"
        else:
            if not has_zero_counts:
                message = f"⚠️  Measles: No zero-count records found. This may indicate zero counts are being filtered out."
            else:
                message = f"⚠️  Measles: Unusual zero count rate: {zero_percentage:.1f}% of records have zero cases"

        metadata = {
            'total_records': total_records,
            'zero_count_records': zero_count_records_count,
            'zero_percentage': zero_percentage,
            'groups_with_zeros': len([g for g, stats in group_stats.items() if stats['zero_records'] > 0]),
            'group_stats': group_stats,
            'disease_types': df['disease'].unique().tolist() if 'disease' in df.columns else []
        }

        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.INFO,
            description=message,
            metadata=metadata
        )

    except Exception as e:
        logger.error(f"Error checking measles zero counts: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Unexpected error during zero counts validation: {e}"
        )
