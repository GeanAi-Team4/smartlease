# controller.py
import subprocess
import os

def run_pipeline(location: str, listing_type: str, past_days: int, start_row: int, end_row: int):
    """
    Orchestrates the full property pipeline with inputs.
    """
    base_dir = os.path.dirname(__file__)  # Directory where this script lives

    # Paths to each script
    scrape_script = os.path.join(base_dir, "scrape_properties.py")
    poi_script = os.path.join(base_dir, "get_poi.py")
    clean_script = os.path.join(base_dir, "data_cleaning.py")
    upsert_script = os.path.join(base_dir, "upsert_snowflake.py")

    # Step 1: Scraping
    subprocess.run([
        "python", scrape_script,
        "--location", location,
        "--listing_type", listing_type,
        "--past_days", str(past_days)
    ], check=True)

    # Step 2: POI fetching
    subprocess.run([
        "python", poi_script,
        "--start_row", str(start_row),
        "--end_row", str(end_row)
    ], check=True)

    # Step 3: Cleaning
    subprocess.run(["python", clean_script], check=True)

    # Step 4: Upsert
    subprocess.run(["python", upsert_script], check=True)

    return "✅ Pipeline executed successfully"
