# Add your imports here
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import pyarrow as pa

import pandas as pd



# Add any utility functions here if needed
def load_tasks(repo_root: Path) -> Dict:
    tasks_path = repo_root / "tasks.json"
    with tasks_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def  group_tasks_by_location_month(tasks: List[Dict]) -> Dict[Tuple[str, str], Dict]:
    """
    Groups tasks by (location, YYYYMM).
    Returns dict with keys (location, yyyymm) and values containing:
      - raw_paths: list of daily raw parquet paths
      - structured_path: single monthly structured parquet path
    """
    grouped: Dict[Tuple[str, str], Dict[str, List[str] | str]] = defaultdict(lambda: {"raw_paths": [], "structured_path": ""})
    for t in tasks:
        location = t["location"]
        # t["date"] is ISO date (YYYY-MM-DD)
        yyyymm = datetime.fromisoformat(t["date"]).strftime("%Y%m")
        key = (location, yyyymm)
        grouped[key]["raw_paths"].append(t["raw_path"])
        # structured_path in tasks.json is already month-resolved for that date
        # Keep the first we see (all in the same group are identical)
        if not grouped[key]["structured_path"]:
            grouped[key]["structured_path"] = t["structured_path"]
    return grouped


def  read_raw_long(paths: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for p in paths:
        path = Path(p)
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
            frames.append(df)
        except Exception as e:
            logging.error("Failed reading raw parquet %s: %s", p, e)
    if not frames:
        return pd.DataFrame(columns=["timestamp", "location", "sensor_name", "value"])
    df_all = pd.concat(frames, ignore_index=True)
    # Ensure expected columns exist
    expected_cols = {"timestamp", "location", "sensor_name", "value"}
    missing = expected_cols - set(df_all.columns)
    if missing:
        raise ValueError(f"Missing columns in raw data: {missing}")
    # Normalize timestamp to ms precision, naive UTC
    # df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], utc=True).astype("datetime64[ms]")
    return df_all


def  long_to_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame(columns=["timestamp", "location"])
    wide = (
        df_long.pivot_table(
            index=["timestamp", "location"],
            columns="sensor_name",
            values="value",
            aggfunc="last",
        )
        .reset_index()
    )
    # Flatten columns (pivot creates MultiIndex on columns)
    wide.columns.name = None
    # Ensure timestamp dtype and ordering
    # wide["timestamp"] = pd.to_datetime(wide["timestamp"], utc=False).astype("datetime64[ms]")
    # Order columns: timestamp, location, sensors (alphabetical)
    fixed_cols = ["timestamp", "location"]
    sensor_cols = sorted([c for c in wide.columns if c not in fixed_cols])
    wide = wide[fixed_cols + sensor_cols]
    wide.rename(columns = {'dew_point_2m':'dew_point'}, inplace= True)
    return wide


def  merge_with_historical(wide_new: pd.DataFrame, structured_path: str) -> pd.DataFrame:
    path = Path(structured_path)
    if path.exists():
        try:
            hist = pd.read_parquet(path)
            # Fix duplicate column names if any (defensive)
            if hist.columns.duplicated().any():
                hist = hist.loc[:, ~hist.columns.duplicated()]
            # Normalize timestamp dtype for safe concat
            # Handle timezone-aware timestamps from historical data
            # hist["timestamp"] = pd.to_datetime(hist["timestamp"], utc=True)
            # Convert timezone-aware to naive UTC (same approach as scraper)
            # hist["timestamp"] = hist["timestamp"].apply(
            #     lambda x: x.tz_convert("UTC").replace(tzinfo=None) if x.tz is not None else x
            # )
            # hist["timestamp"] = hist["timestamp"].astype("datetime64[ms]")
            hist["timestamp"] = hist["timestamp"].dt.round("ms")

        except Exception as e:
            logging.error("Failed reading historical parquet %s: %s", structured_path, e)
            hist = pd.DataFrame(columns=wide_new.columns)
    else:
        hist = pd.DataFrame(columns=wide_new.columns)

    # Handle empty DataFrames to avoid FutureWarning
    # Align columns before concatenation (hist and new might have different sensors)
    if hist.empty:
        combined = wide_new.copy()
    elif wide_new.empty:
        combined = hist.copy()
    else:
        # Ensure both have the same columns (fill missing with NaN)
        # Keep order: timestamp, location, then sensors (alphabetically sorted)
        fixed_cols = ["timestamp", "location"]
        all_sensors = sorted(set(hist.columns) | set(wide_new.columns) - set(fixed_cols))
        all_columns = fixed_cols + all_sensors
        hist = hist.reindex(columns=all_columns)
        wide_new = wide_new.reindex(columns=all_columns)
        combined = pd.concat([hist, wide_new], ignore_index=True)
    
    # Fix duplicate column names if any (defensive)
    if combined.columns.duplicated().any():
        combined = combined.loc[:, ~combined.columns.duplicated()]
    
    # Drop duplicates on (timestamp, location), keep the latest
    combined = combined.sort_values("timestamp")
    combined = combined.drop_duplicates(subset=["timestamp", "location"], keep="last")
    # Columns are already in correct order from above, but ensure consistency
    fixed_cols = ["timestamp", "location"]
    sensor_cols = sorted([c for c in combined.columns if c not in fixed_cols])
    combined = combined[fixed_cols + sensor_cols]
    
    return combined


def  write_structured(df: pd.DataFrame, path_str: str) -> None:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # df.to_parquet(path, index=False)
    table = pa.Table.from_pandas(df, preserve_index=False)   # preserve_index as needed
    pa.parquet.write_table(table, path, coerce_timestamps="ms")


def transform():
    # Implement the transform logic here
    # 1. Load tasks.json to get the list of dates and locations to process
    # 2. Load all raw LONG format parquet files for the date range
    # 3. Convert LONG format to WIDE format (pivot sensor_name into columns)
    # 4. Load existing historical data from structured_output_dir
    # 5. Merge new data with historical data (handle duplicates and schema differences)
    # 6. Write monthly parquet files to structured_output_dir

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    repo_root = Path(__file__).resolve().parents[2]  # project root
    cfg =  load_tasks(repo_root)

    tasks = cfg.get("tasks", [])
    if not tasks:
        logging.info("No tasks found in tasks.json; nothing to transform.")
        return

    groups =  group_tasks_by_location_month(tasks)

    processed = 0
    skipped = 0
    for (location, yyyymm), info in groups.items():
        raw_paths = sorted(set(info["raw_paths"]))
        structured_path = info["structured_path"]

        df_long =  read_raw_long(raw_paths)
        if df_long.empty:
            skipped += 1
            logging.info("No raw data for %s %s; skipping.", location, yyyymm)
            continue

        wide_new =  long_to_wide(df_long)
        merged =  merge_with_historical(wide_new, structured_path)
        try:
            write_structured(merged, structured_path)
            processed += 1
            logging.info(
                "Wrote %s for %s %s (%d rows)",
                structured_path,
                location,
                yyyymm,
                len(merged),
            )
        except Exception as e:
            logging.error("Failed writing structured parquet %s: %s", structured_path, e)

    logging.info("Transform done. processed=%d skipped=%d", processed, skipped)


if __name__ == "__main__":
    transform()