import pandas as pd
from datetime import datetime, timedelta

def generate_weekly_data(start_year=2020, end_year=2027):
    """
    Generate year, week number, and start date for each week from start_year to end_year.
    Weeks start on Sunday and follow ISO week numbering adjusted for Sunday start.
    """
    results = []

    for year in range(start_year, end_year + 1):
        # Start from January 1st of the year
        current_date = pd.Timestamp(year, 1, 1)

        # Find the first Sunday of the year or use Jan 1 if it's already Sunday
        # Sunday = 6 in pandas (Monday = 0)
        days_until_sunday = (6 - current_date.weekday()) % 7
        first_sunday = current_date + pd.Timedelta(days=days_until_sunday)

        # If Jan 1 is not Sunday and first Sunday is more than 3 days away,
        # consider the week containing Jan 1 as week 1
        if current_date.weekday() != 6 and days_until_sunday > 3:
            # Start from the Sunday before Jan 1
            first_sunday = current_date - pd.Timedelta(days=(current_date.weekday() + 1) % 7)

        week_num = 1
        week_start = first_sunday

        # Generate weeks for the entire year
        while week_start.year <= year:
            if week_start.year == year or (week_start.year == year - 1 and week_start + pd.Timedelta(days=6) >= pd.Timestamp(year, 1, 1)):
                results.append({
                    'year': year,
                    'week': week_num,
                    'week_start_date': week_start.date()
                })

            week_start += pd.Timedelta(days=7)
            week_num += 1

            # Stop if we've moved to the next year and it's more than a few days in
            if week_start.year > year and week_start > pd.Timestamp(year + 1, 1, 7):
                break

    return pd.DataFrame(results)


def check_missing_weeks(df, date_column='WkStrtActual'):
    """
    Check for missing weeks in a time series dataset.

    Parameters:
    df (Dataframe):
    date_column (str): Name of the column containing week start dates

    Returns:
    dict: Summary of missing weeks analysis
    """

    # Check if the date column exists
    if date_column not in df.columns:
        available_cols = [col for col in df.columns if
                          'date' in col.lower() or 'week' in col.lower() or 'time' in col.lower()]
        return {"error": f"Column '{date_column}' not found. Available date-like columns: {available_cols}"}

    # Convert date column to datetime
    df[date_column] = pd.to_datetime(df[date_column])

    # Sort by date to ensure proper order
    df = df.sort_values(date_column)

    # Get unique week start dates
    unique_weeks = df[date_column].drop_duplicates().sort_values()

    # Get the date range
    min_date = unique_weeks.min()
    max_date = unique_weeks.max()

    print(f"Date range: {min_date.date()} to {max_date.date()}")
    print(f"Number of unique weeks in data: {len(unique_weeks)}")

    # Generate expected weekly sequence
    # Assuming weeks start on the same day of week as the first date
    expected_weeks = []
    current_week = min_date

    while current_week <= max_date:
        expected_weeks.append(current_week)
        current_week += timedelta(days=7)

    expected_weeks = pd.Series(expected_weeks)
    print(f"Expected number of weeks: {len(expected_weeks)}")

    # Find missing weeks
    missing_weeks = expected_weeks[~expected_weeks.isin(unique_weeks)]

    # Find extra weeks (shouldn't happen in a proper weekly sequence, but checking)
    extra_weeks = unique_weeks[~unique_weeks.isin(expected_weeks)]

    # Results summary
    results = {
        "total_rows": len(df),
        "unique_weeks_in_data": len(unique_weeks),
        "expected_weeks": len(expected_weeks),
        "missing_weeks_count": len(missing_weeks),
        "extra_weeks_count": len(extra_weeks),
        "date_range": (min_date.date(), max_date.date()),
        "missing_weeks": missing_weeks.dt.date.tolist() if len(missing_weeks) > 0 else [],
        "extra_weeks": extra_weeks.dt.date.tolist() if len(extra_weeks) > 0 else [],
        "data_sample": df.head()
    }

    return results
