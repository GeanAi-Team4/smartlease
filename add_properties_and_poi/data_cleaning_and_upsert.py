# data_cleaning_and_upsert.py
# This script reads the properties data with POI information from the properties_with_poi.csv file, performs basic data analysis (like counting POIs),
# and prepares the data for upsert to Snowflake. For now, it just prints some stats instead of upserting.

import pandas as pd
import snowflake.connector

# Snowflake credentials
SNOWFLAKE_ACCOUNT = "SFEDU02-PDB57018"
SNOWFLAKE_USER = "BADGER"
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_AUTHENTICATOR = "externalbrowser"
SNOWFLAKE_ROLE = "TRAINING_ROLE"
SNOWFLAKE_DATABASE = "LISTINGS"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "LISTINGS_WH"

def print_stats(properties):
    """
    Prints basic stats about the property data.
    """
    total_properties = len(properties)
    avg_pois_per_property = properties['Nearby_POIs'].apply(lambda x: len(x.split(',')) if isinstance(x, str) else 0).mean()
    
    print(f"Total Properties: {total_properties}")
    print(f"Average Number of POIs per Property: {avg_pois_per_property:.2f}")

def upsert_to_snowflake(properties):
    """
    Connects to Snowflake and prepares data for upsert (just prints a mock-up for now).
    """
    try:
        # Establish connection to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            authenticator=SNOWFLAKE_AUTHENTICATOR
        )

        cursor = conn.cursor()

        # Create a new table for this data (if it doesn't already exist)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS property_data_with_poi (
                address STRING,
                listing_type STRING,
                past_days INT,
                nearby_pois STRING
            );
        """)

        # Print mock up for inserting data
        for _, row in properties.iterrows():
            address = row['address']
            listing_type = row['listing_type']
            past_days = row['past_days']
            nearby_pois = row['Nearby_POIs']
            print(f"Upserting to Snowflake: {address}, {listing_type}, {past_days}, {nearby_pois}")

        cursor.close()
        conn.close()
        print("Data upserted to Snowflake.")
    except Exception as e:
        print(f"Error while connecting to Snowflake: {e}")

if __name__ == "__main__":
    # Example usage
    input_csv_poi = "/path/to/add_properties/output/properties_with_poi.csv"
    
    # Step 1: Load the properties data with POIs
    properties = pd.read_csv(input_csv_poi)

    # Step 2: Print basic stats
    print_stats(properties)

    # Step 3: Upsert data (mock-up for now)
    upsert_to_snowflake(properties)
