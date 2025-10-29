#!/usr/bin/env python3
"""
Weather Data Pipeline CLI

This CLI provides commands to run the weather data ingestion pipeline.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src import parametrize
from src.scraper import openmeteo as scraper
from src.transform import openmeteo as transform


def run_parametrize():
    parametrize.parametrize()


def run_scrape():
    scraper.scrape()


def run_transform():
    transform.transform()


def run_pipeline():
    run_parametrize()
    run_scrape()
    run_transform()


def main():
    if len(sys.argv) < 2:
        sys.exit(1)

    command = sys.argv[1].lower()

    commands = {
        "parametrize": run_parametrize,
        "scrape": run_scrape,
        "transform": run_transform,
        "pipeline": run_pipeline,
    }

    if command not in commands:
        sys.exit(1)

    commands[command]()


if __name__ == "__main__":
    main()
