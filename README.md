# Weather Data Pipeline Challenge

**Time Limit**: 95 minutes
**Language**: Python 3.11+

---

## Overview

Build a production-grade data ingestion pipeline that extracts weather data from the Open-Meteo API, transforms it, and merges it with existing historical data in a simulated data lake environment.

This challenge evaluates your ability to:
- Design and implement ETL pipelines
- Handle configuration-driven architecture
- Work with multiple data formats
- Merge incremental data with existing datasets
- Handle errors gracefully (log errors, continue processing)
- Implement clear and useful logging
- Make sure your solution is readable and well-organized
- Include any assumptions or design decisions in code comments or in this README.md

---

## Architecture

Your pipeline consists of three stages:

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ PARAMETRIZE  │  →   │   SCRAPE     │  →   │  TRANSFORM   │
└──────────────┘      └──────────────┘      └──────────────┘
```

**Stage 1: Parametrize**
Load configuration and prepare pipeline parameters

**Stage 2: Scrape**
Extract data from external API and store as raw data

**Stage 3: Transform**
Clean, transform, and merge data into structured format

---

## Setup

### Prerequisites
- Python 3.11+
- `uv` package manager (recommended for faster dependency management)

### Installation

1. Install `uv` if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies:
```bash
uv sync
```

### Running the Pipeline

The CLI provides four commands:

```bash
# Run individual stages
uv run cli.py parametrize    # Generate task list from config
uv run cli.py scrape         # Fetch weather data from API
uv run cli.py transform      # Transform and merge data

# Run the complete pipeline
uv run cli.py pipeline       # Execute all stages sequentially
```

---

## What You're Given

### Project Structure
```
candidate-template/
├── cli.py                      # Main entry point (empty)
├── workload.json               # Configuration file (provided)
├── src/
│   ├── parametrize.py         # Empty - you implement
│   ├── scraper/
│   │   └── openmeteo.py       # Empty - you implement
│   └── transform/
│       └── openmeteo.py       # Empty - you implement
└── data/
    └── structured/            # Existing historical data
        ├── amsterdam/202410.parquet
        └── london/202410.parquet
```

### Existing Historical Data

You have existing weather data for October 2024 in `data/structured/{location}/202410.parquet`.

**Locations**: amsterdam, london
**Format**: Parquet files in WIDE format
**Columns**: timestamp, location, temperature, humidity, wind_speed, pressure, clouds, dew_point

This represents historical data already in the data lake that your new data must merge with.

---

## Requirements

### 1. Parametrize Module

**File**: `src/parametrize.py`

**Requirements**:
- Load and validate `workload.json` configuration file
- Parse ISO 8601 duration format in `time_increment` field
  - Example: `+P1DT00H00M00S` represents a 1-day increment
  - Format: `+P[days]DT[hours]H[minutes]M[seconds]S`
- Generate list of dates for each location, between `begin_date` and `end_date` using the time increment
  - Store this list in `tasks.json` file so it is used in the next stages
- Both `raw_output_dir` and `structured_output_dir` in `workload.json` are path templates with placeholders:
  - `{location_name}` is replaced by the location (e.g., `amsterdam`)
  - `%Y%m%d` or `%Y%m` are date formats via Python's `strftime`

**Configuration Schema**:
```json
{
  "date_config": {
    "begin_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "time_increment": "+P1DT00H00M00S"
  },
  "locations": [
    {"name": "...", "sensors": [...]}
  ],
  "local_storage": {
    "raw_output_dir": "...",
    "structured_output_dir": "..."
  }
}
```

---

### 2. Scraper Module

**File**: `src/scraper/openmeteo.py`

**Requirements**:
- Fetch historical weather data from Open-Meteo Archive API
- Process data for all locations defined in configuration
- Convert API response to LONG format with columns: `timestamp`, `location`, `sensor_name`, `value`
- Write daily parquet files to local_storage.raw_output_dir from workload.json

**API Endpoint**: `https://archive-api.open-meteo.com/v1/archive`

**Location Coordinates**:
| Location | Latitude | Longitude |
|----------|----------|-----------|
| amsterdam | 52.37 | 4.89 |
| london | 51.51 | -0.13 |

**Output Format**: LONG format (one row per sensor reading)
```
timestamp, location, sensor_name, value
```

---

### 3. Transform Module

**File**: `src/transform/openmeteo.py`

**Requirements**:
- Load all raw LONG format files for a given date range
- Convert raw LONG format data to WIDE format
- Merge WIDE format data with existing historical data in `data/structured/`
- Write monthly parquet files to local_storage.structured_output_dir from workload.json
- Ensure data quality and consistency

**Output Format**: WIDE format (one row per timestamp)
```
timestamp, location, temperature_2m, relative_humidity_2m, wind_speed_10m, surface_pressure, cloud_cover, [other_sensors]
```

---

## Expected Outputs

After running the complete pipeline:

```
data/
├── raw/                           # LONG format, daily files
│   ├── amsterdam/
│   │   ├── 20241016.parquet
│   │   ├── 20241017.parquet
│   │   └── 20241018.parquet
│   └── london/
│       ├── 20241016.parquet
│       ├── 20241017.parquet
│       └── 20241018.parquet
│
└── structured/                    # WIDE format, monthly files
    ├── amsterdam/
    │   └── 202410.parquet
    └── london/
        └── 202410.parquet
```

**Structured Data Requirements**:
- Contains both historical data (Oct 1-15) and new data (Oct 16-18)
- No duplicate (timestamp, location) pairs
- Timestamps in millisecond precision UTC timezone
- All columns from both historical and new data present

---

Good luck! 🚀
