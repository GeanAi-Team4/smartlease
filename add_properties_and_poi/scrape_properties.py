# scrape_properties.py
# This script uses the homeharvest library to scrape property data for a given location, listing type, and date range.
# The data is saved to a CSV file in the specified output directory and adds a new 'address' column.

from homeharvest import scrape_property
import os
import pandas as pd

def run_scraper(location="Boston, MA", listing_type="for_rent", past_days=1):
    """
    Scrapes property data and saves it to a CSV file.
    
    Parameters:
    - location (str): Location for property search.
    - listing_type (str): Type of property listings (e.g., 'for_rent', 'for_sale').
    - past_days (int): Number of past days to filter the listings by.
    """
    print(f"▶ Scraping properties in {location} for '{listing_type}' (past {past_days} days)")

    # Define output directory
    output_dir = "/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties/output"
    os.makedirs(output_dir, exist_ok=True)

    # Define the path for the CSV file
    output_path = os.path.join(output_dir, "properties_raw.csv")
    print(f"▶ Output path will be: {output_path}")

    # Scrape the property data
    properties = scrape_property(
        location=location,
        listing_type=listing_type,
        past_days=past_days
    )

    # Add the 'address' column by combining relevant columns (full_street_line, city, state, zip_code)
    properties['address'] = properties['full_street_line'] + ', ' + properties['city'] + ', ' + properties['state'] + ' ' + properties['zip_code']

    # Save the data to CSV with the new 'address' column
    properties.to_csv(output_path, index=False)
    print(f"Saved {len(properties)} properties to {output_path}")
    print(f"File written: {os.path.exists(output_path)}")

# Example usage (this will be run when the script is executed directly):
run_scraper(location="Boston, MA", listing_type="for_rent", past_days=5)
