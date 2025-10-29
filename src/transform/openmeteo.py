# Add your imports here


# Add any utility functions here if needed


def transform():
    # Implement the transform logic here
    # 1. Load tasks.json to get the list of dates and locations to process
    # 2. Load all raw LONG format parquet files for the date range
    # 3. Convert LONG format to WIDE format (pivot sensor_name into columns)
    # 4. Load existing historical data from structured_output_dir
    # 5. Merge new data with historical data (handle duplicates and schema differences)
    # 6. Write monthly parquet files to structured_output_dir
    raise NotImplementedError
