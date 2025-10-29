# Add your imports here


# Add any utility functions here if needed


def parametrize():
    # Implement the parametrize logic here
    # 1. Load and validate workload.json configuration file
    # 2. Parse ISO 8601 duration format from time_increment field (e.g., +P1DT00H00M00S)
    # 3. Generate list of dates between begin_date and end_date using time_increment
    # 4. Create tasks for each location and date combination
    # 5. Write tasks to tasks.json file for use in scrape and transform stages
    raise NotImplementedError
