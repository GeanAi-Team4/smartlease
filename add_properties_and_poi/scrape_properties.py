from homeharvest import scrape_property
import os
import pandas as pd

def run_scraper(location="Boston, MA", listing_type="for_rent", past_days=20):
    print(f"▶ Scraping properties in {location} for '{listing_type}' (past {past_days} days)")

    output_dir = "/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties_and_poi/output"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "properties_raw.csv")
    print(f"▶ Output path will be: {output_path}")

    try:
        properties = scrape_property(
            location=location,
            listing_type=listing_type,
            past_days=past_days
        )
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        return 0, None

    if properties.empty:
        print("⚠️ No properties found. Exiting early.")
        return 0, None

    properties['address'] = (
        properties['full_street_line'] + ', ' + 
        properties['city'] + ', ' + 
        properties['state'] + ' ' + 
        properties['zip_code']
    )

    properties.to_csv(output_path, index=False)
    print(f"✅ Saved {len(properties)} properties to {output_path}")
    return len(properties), output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--location", type=str, default="Boston, MA")
    parser.add_argument("--listing_type", type=str, default="for_rent")
    parser.add_argument("--past_days", type=int, default=3)

    args = parser.parse_args()

    run_scraper(
        location=args.location,
        listing_type=args.listing_type,
        past_days=args.past_days
    )

