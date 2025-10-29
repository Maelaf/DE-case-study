# Add your imports here
import json
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, Field, ValidationError, field_validator

# Add any utility functions here if needed
class DateConfig(BaseModel):
    begin_date: date
    end_date: date
    time_increment: str

    @field_validator("end_date")
    @classmethod
    def validate_range(cls, v: date, info):
        begin = info.data.get("begin_date")
        if begin and v < begin:
            raise ValueError("end_date must be on or after begin_date")
        return v


class Location(BaseModel):
    name: str = Field(min_length=1)
    sensors: List[str] = Field(min_length=1)


class LocalStorage(BaseModel):
    raw_output_dir: str = Field(min_length=1)
    structured_output_dir: str = Field(min_length=1)


class WorkloadConfig(BaseModel):
    date_config: DateConfig
    locations: List[Location] = Field(min_length=1)
    local_storage: LocalStorage


_DURATION_RE = re.compile(
    r"^[+-]?P(?:(?P<days>\d+)D)?T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?$"
)


def parse_iso8601_duration(duration: str) -> timedelta:
    match = _DURATION_RE.match(duration)
    if not match:
        raise ValueError(f"Invalid ISO8601 duration: {duration}")
    parts = {k: int(v) if v is not None else 0 for k, v in match.groupdict().items()}
    delta = timedelta(
        days=parts["days"], hours=parts["hours"], minutes=parts["minutes"], seconds=parts["seconds"]
    )
    if delta <= timedelta(0):
        raise ValueError("time_increment must be a positive duration")
    return delta


def build_date_range(begin: date, end: date, step: timedelta) -> List[date]:
    current = datetime.combine(begin, datetime.min.time()).date()
    last = datetime.combine(end, datetime.min.time()).date()
    dates: List[date] = []
    while current <= last:
        dates.append(current)
        current = (datetime.combine(current, datetime.min.time()) + step).date()
    return dates


def materialize_path(template: str, location_name: str, d: date) -> str:
    templated = template.replace("{location_name}", location_name)
    return d.strftime(templated)


def parametrize():
#     # Implement the parametrize logic here
#     # 1. Load and validate workload.json configuration file
#     # 2. Parse ISO 8601 duration format from time_increment field (e.g., +P1DT00H00M00S)
#     # 3. Generate list of dates between begin_date and end_date using time_increment
#     # 4. Create tasks for each location and date combination
#     # 5. Write tasks to tasks.json file for use in scrape and transform stages
#     raise NotImplementedError

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    repo_root = Path(__file__).resolve().parent.parent
    config_path = repo_root / "workload.json"
    tasks_path = repo_root / "tasks.json"

    try:
        with config_path.open("r", encoding="utf-8") as f:
            raw_config: Dict = json.load(f)
        config = WorkloadConfig.model_validate(raw_config)
    except (OSError, json.JSONDecodeError, ValidationError) as e:
        logging.error("Failed to load/validate workload.json: %s", e)
        raise

    step = parse_iso8601_duration(config.date_config.time_increment)
    dates = build_date_range(
        config.date_config.begin_date, config.date_config.end_date, step
    )

    tasks: List[Dict] = []
    for loc in config.locations:
        for d in dates:
            tasks.append(
                {
                    "location": loc.name,
                    "date": d.isoformat(),
                    "raw_path": materialize_path(
                        config.local_storage.raw_output_dir, loc.name, d
                    ),
                    "structured_path": materialize_path(
                        config.local_storage.structured_output_dir, loc.name, d.replace(day=1)
                    ),
                }
            )

    output = {
        "date_config": {
            "begin_date": config.date_config.begin_date.isoformat(),
            "end_date": config.date_config.end_date.isoformat(),
            "time_increment": config.date_config.time_increment,
        },
        "locations": [loc.model_dump() for loc in config.locations],
        "local_storage": config.local_storage.model_dump(),
        "tasks": tasks,
    }

    try:
        with tasks_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        logging.info(
            "Generated %d tasks across %d locations into %s",
            len(tasks),
            len(config.locations),
            tasks_path,
        )
    except OSError as e:
        logging.error("Failed to write tasks.json: %s", e)
        raise
