# # Add your imports here
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

# # Add any utility functions here if needed



OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

LOCATION_COORDS = {
    "amsterdam": {"latitude": 52.37, "longitude": 4.89},
    "london": {"latitude": 51.51, "longitude": -0.13},
}


def load_tasks(repo_root: Path) -> Dict:
    tasks_path = repo_root / "tasks.json"
    with tasks_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_sensors_for_location(config_locations: List[Dict], location_name: str) -> List[str]:
    for loc in config_locations:
        if loc["name"] == location_name:
            return list(loc["sensors"])
    raise ValueError(f"No sensors configured for location: {location_name}")


def fetch_day(location: str, date_str: str, sensors: List[str]) -> Dict:
    
    MAX_RETRIES = 3
    RETRY_DELAY = 2 # seconds
    coords = LOCATION_COORDS.get(location)
    if not coords:
        raise ValueError(f"Coordinates missing for location: {location}")

    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "start_date": date_str,
        "end_date": date_str,
        "hourly": ",".join(sensors),
        "timezone": "UTC",
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(OPEN_METEO_URL, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            raise requests.HTTPError(f"HTTP error for {location} {date_str}: {e}") from e
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY)
    return response.json()


def json_to_long(df_json: Dict, location: str) -> pd.DataFrame:
    hourly = df_json.get("hourly", {})
    times = hourly.get("time")
    if not times:
        return pd.DataFrame(columns=["timestamp", "location", "sensor_name", "value"])

    # Build wide DataFrame from hourly dict
    data = {k: v for k, v in hourly.items() if k != "time"}
    wide = pd.DataFrame({"time": times, **data})

    # Normalize timestamp to UTC (as naive datetime for parquet compatibility)
    wide["timestamp"] = pd.to_datetime(wide["time"], utc=True)
    
    wide = wide.drop(columns=["time"])

    # Melt to long
    long_df = wide.melt(id_vars=["timestamp"], var_name="sensor_name", value_name="value")
    long_df["location"] = location

    # Reorder and ensure dtype
    long_df = long_df[["timestamp", "location", "sensor_name", "value"]]
    # Convert timezone-aware to naive UTC (datetime64[ms] doesn't support timezone-aware)
    # Convert each timestamp: ensure UTC, then make naive
    # long_df["timestamp"] = long_df["timestamp"].apply(
    #     lambda x: x.tz_convert("UTC").replace(tzinfo=None) if x.tz is not None else x
    # )
    # long_df["timestamp"] = long_df["timestamp"].astype("datetime64[ms]")
    # long_df.info()

    return long_df


def write_parquet(df: pd.DataFrame, path_str: str) -> None:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as e:
        # Likely missing pyarrow/fastparquet
        raise RuntimeError(
            "Failed to write parquet. Ensure 'pyarrow' is installed: "
            "uv add pyarrow"
        ) from e



def scrape():
#     # Implement the API scrape logic here
#     # 1. Load tasks.json to get the list of dates and locations to scrape
#     # 2. Fetch data from Open-Meteo Archive API for each task
#     # 3. Convert API response to LONG format (timestamp, location, sensor_name, value)
#     # 4. Write daily parquet files to raw_output_dir
#     raise NotImplementedError

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    repo_root = Path(__file__).resolve().parents[2]  # project root
    config = load_tasks(repo_root)

    tasks = config.get("tasks", [])
    locations_cfg = config.get("locations", [])

    total = 0
    skipped = 0
    errors = 0
    errored = []

    for task in tasks:
        location = task["location"]
        date_str = task["date"]
        raw_path = task["raw_path"]

        # Idempotency: skip if file exists
        if Path(raw_path).exists():
            skipped += 1
            logging.info("Skip existing file: %s", raw_path)
            continue

        try:
            sensors = get_sensors_for_location(locations_cfg, location)
            payload = fetch_day(location, date_str, sensors)
            df_long = json_to_long(payload, location)

            # Even if empty, still create the file to mark processed
            write_parquet(df_long, raw_path)
            total += 1
            logging.info("Wrote %s (%d rows)", raw_path, len(df_long))
        except requests.HTTPError as e:
            errored.append(date_str)
            errors += 1
            logging.error("HTTP error for %s %s: %s", location, date_str, e)
            
        except RuntimeError as e:  # catches fetch_day retry failure
            errors += 1
            logging.error("Failed to fetch data for %s %s after retries: %s", location, date_str, e)

        
        except Exception as e:
            errors += 1
            logging.error("Failed for %s %s: %s", location, date_str, e)

    logging.info("Scrape done. wrote=%d skipped=%d errors=%d errored=%s", total, skipped, errors, errored)


if __name__ == "__main__":
    scrape()