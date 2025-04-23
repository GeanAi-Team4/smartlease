# controller.py
# This script orchestrates the execution of the scraping, POI fetching, and data upsert processes.

import subprocess

def run_scraping_script():
    """
    Runs the scraping script to fetch property data.
    """
    print("Running the scraping script...")
    subprocess.run(["python", "scrape_properties.py"], check=True)

def run_poi_script():
    """
    Runs the POI fetching script to extend property data with POIs.
    """
    print("Running the POI fetching script...")
    subprocess.run(["python", "get_poi.py"], check=True)

def run_data_cleaning_and_upsert_script():
    """
    Runs the data cleaning and upsert script to process data and upsert it to Snowflake.
    """
    print("Running the data cleaning and upsert script...")
    subprocess.run(["python", "data_cleaning_and_upsert.py"], check=True)

if __name__ == "__main__":
    # Step 1: Run the scraping script
    run_scraping_script()

    # Step 2: Run the POI script
    run_poi_script()

    # Step 3: Run the data cleaning and upsert script
    run_data_cleaning_and_upsert_script()

    print("All steps completed successfully.")
