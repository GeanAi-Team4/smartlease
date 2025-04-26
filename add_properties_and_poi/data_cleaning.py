import pandas as pd
import toml

# Load the .toml config file
config = toml.load("config.toml")

# Load the dataset
input_csv = config["paths_step_3"]['input_csv_with_poi']  # Path from the TOML file
output_csv = config["paths_step_3"]['output_csv_with_poi_clean']  # Path for the cleaned output CSV

# Load the dataset
properties = pd.read_csv(input_csv)

# Columns to keep
columns_to_keep = [
    'property_id', 'address', 'status', 'style', 'beds', 'full_baths', 'sqft', 'year_built', 'list_price',
    'latitude', 'longitude', 'neighborhoods', 'county', 'nearby_schools', 'primary_photo', 'alt_photos',
    'restaurant_name', 'restaurant_rating', 'restaurant_address', 'cafe_name', 'cafe_rating', 'cafe_address',
    'hospital_name', 'hospital_rating', 'hospital_address', 'pharmacy_name', 'pharmacy_rating', 'pharmacy_address',
    'atm_name', 'atm_rating', 'atm_address', 'bank_name', 'bank_rating', 'bank_address'
]

# Retain only necessary columns
properties_cleaned = properties[columns_to_keep]

# Create the 'complete_property_details' column by concatenating all other columns
properties_cleaned['complete_property_details'] = properties_cleaned.apply(lambda row: ', '.join(row.astype(str)), axis=1)

# Save the cleaned data to the new CSV file
properties_cleaned.to_csv(output_csv, index=False)
print(f"Data cleaned and saved to {output_csv}")
