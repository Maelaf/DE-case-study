# Add your imports here


# Add any utility functions here if needed


def scrape():
    # Implement the API scrape logic here
    # 1. Load tasks.json to get the list of dates and locations to scrape
    # 2. Fetch data from Open-Meteo Archive API for each task
    # 3. Convert API response to LONG format (timestamp, location, sensor_name, value)
    # 4. Write daily parquet files to raw_output_dir
    raise NotImplementedError
