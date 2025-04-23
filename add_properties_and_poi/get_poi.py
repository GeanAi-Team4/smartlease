import os
import pandas as pd
import requests
import time
import dotenv
from math import radians, cos, sin, asin, sqrt

# Load environment variables from .env file
dotenv.load_dotenv()

# POI types to search for
DEFAULT_POI_TYPES = ['restaurant', 'cafe', 'hospital', 'pharmacy', 'atm', 'bank']

def get_poi_details(poi_id, api_key):
    """
    Get detailed information about a specific POI using the Google Places API's Place Details endpoint.

    Parameters:
    - poi_id (str): The Google Places ID of the place.
    - api_key (str): Google Places API key.

    Returns:
    - Dictionary containing detailed POI information.
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/details/json"
    url = f"{endpoint}?place_id={poi_id}&key={api_key}"

    try:
        # Send request to Google Places API for place details
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        result = response.json()

        # Extract detailed information
        if 'result' in result:
            place = result['result']
            details = {
                'name': place.get('name', 'N/A'),
                'address': place.get('formatted_address', 'N/A'),
                'rating': place.get('rating', 'N/A'),
                'user_ratings_total': place.get('user_ratings_total', 'N/A'),
                'types': place.get('types', []),
                'vicinity': place.get('vicinity', 'N/A')
            }
            return details
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching details for POI {poi_id}: {e}")
        return None

def get_poi_for_address(address, api_key, lat=None, lng=None):
    """
    Get Points of Interest (POI) for a given address using the Google Places API.

    Parameters:
    - address (str): The address of the property.
    - api_key (str): Google Places API key.
    - lat (float, optional): Latitude of the property (if using lat/lng instead of address).
    - lng (float, optional): Longitude of the property (if using lat/lng instead of address).

    Returns:
    - List of detailed information for nearby POIs.
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    
    # If lat/lng are provided, use them, otherwise, use the address.
    if lat and lng:
        location = f"{lat},{lng}"
        query = f"location={location}&radius=1000"  # 1000 meters radius
    else:
        query = f"query=near {address}"
    
    url = f"{endpoint}?{query}&key={api_key}"

    try:
        # Send request to Google Places API for nearby POIs
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        results = response.json()

        # Collect detailed POIs from the response
        poi_details = []
        if 'results' in results:
            for result in results['results']:
                poi_id = result['place_id']
                details = get_poi_details(poi_id, api_key)
                if details:
                    poi_details.append(details)

        # Return the detailed POIs
        return poi_details

    except requests.exceptions.RequestException as e:
        # Log the error and return an empty list
        print(f"Error fetching POIs for address {address}: {e}")
        return []

def add_poi_to_properties(input_csv, output_csv, api_key, start_row=0, end_row=None):
    """
    Adds Points of Interest (POI) data to the properties in the input CSV and saves the extended data to output CSV.
    
    Parameters:
    - input_csv (str): Path to the input CSV file containing property data.
    - output_csv (str): Path to the output CSV file where extended data will be saved.
    - api_key (str): Google Places API key.
    - start_row (int): The starting index of the rows to process (default is 0).
    - end_row (int): The ending index of the rows to process (default is None, which means till the last row).
    """
    # Load the scraped property data from CSV
    properties = pd.read_csv(input_csv)
    
    # Slice the dataframe to select the rows to process
    properties_to_process = properties.iloc[start_row:end_row] if end_row else properties.iloc[start_row:]
    
    # Add new columns for POI data
    properties_to_process['Nearby_POIs'] = ''
    
    # Add POI data with retries and delay between requests
    def get_poi_with_retry(address, lat=None, lng=None):
        retries = 3
        for _ in range(retries):
            pois = get_poi_for_address(address, api_key, lat, lng)
            if pois:
                return pois
            time.sleep(2)  # Wait before retrying
        return []  # After retries, return an empty list if all attempts fail

    # Apply the POI fetching function to each row
    for idx, row in properties_to_process.iterrows():
        property_id = row.get('property_id', 'unknown')
        
        if 'latitude' in row and 'longitude' in row and row['latitude'] and row['longitude']:
            lat = float(row['latitude'])
            lng = float(row['longitude'])
            address = get_address_from_row(row)
            
            print(f"Processing: {address} (ID: {property_id})")
            
            # Get POIs for the property
            pois = get_poi_with_retry(address, lat, lng)
            
            # Join the list of POIs into a detailed format and store in the 'Nearby_POIs' column
            poi_details = []
            for poi in pois:
                poi_details.append(f"{poi['name']} (Rating: {poi['rating']}, Address: {poi['address']})")
            
            properties_to_process.at[idx, 'Nearby_POIs'] = '; '.join(poi_details) if poi_details else "No POIs found"
        else:
            print(f"Skipping: No lat/lng for property ID: {property_id}")
    
    # Save the extended data to a new CSV
    properties_to_process.to_csv(output_csv, index=False)
    print(f"POIs added. Data saved to {output_csv}")

def get_address_from_row(row):
    """Extract the complete address from a CSV row"""
    address_parts = []
    
    # Ensure all fields are converted to strings if they're not already
    if 'street' in row and row['street']:
        address_parts.append(str(row['street']))
    if 'unit' in row and row['unit']:
        address_parts.append(str(row['unit']))
    if 'city' in row and row['city']:
        address_parts.append(str(row['city']))
    if 'state' in row and row['state']:
        address_parts.append(str(row['state']))
    if 'zip_code' in row and row['zip_code']:
        address_parts.append(str(row['zip_code']))
    
    return ", ".join(address_parts) if address_parts else None

# # Usage

api_key = ''
input_csv = '/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties/output/properties_raw.csv'
output_csv = '/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties/output/properties_with_poi.csv'

# Specify the row slice (for example, rows 0-1000)
start_row = 0
end_row = 10

add_poi_to_properties(input_csv=input_csv, output_csv=output_csv, api_key=api_key, start_row=start_row, end_row=end_row)















# import os
# import pandas as pd
# import requests
# import time
# import dotenv
# from math import radians, cos, sin, asin, sqrt

# # Load environment variables from .env file
# dotenv.load_dotenv()

# # POI types to search for
# DEFAULT_POI_TYPES = ['restaurant', 'cafe', 'hospital', 'pharmacy', 'atm', 'bank']

# def get_poi_for_address(address, api_key, lat=None, lng=None):
#     """
#     Get Points of Interest (POI) for a given address using the Google Places API.

#     Parameters:
#     - address (str): The address of the property.
#     - api_key (str): Google Places API key.
#     - lat (float, optional): Latitude of the property (if using lat/lng instead of address).
#     - lng (float, optional): Longitude of the property (if using lat/lng instead of address).

#     Returns:
#     - List of nearby POIs as a string.
#     """
#     endpoint = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    
#     # If lat/lng are provided, use them, otherwise, use the address.
#     if lat and lng:
#         location = f"{lat},{lng}"
#         query = f"location={location}&radius=1000"  # 1000 meters radius
#     else:
#         query = f"query=near {address}"
    
#     url = f"{endpoint}?{query}&key={api_key}"

#     try:
#         # Send request to Google Places API
#         response = requests.get(url)
#         response.raise_for_status()  # Raise an exception for HTTP errors
#         results = response.json()

#         # Collect POIs from the response
#         poi_list = []
#         if 'results' in results:
#             for result in results['results']:
#                 poi_list.append(result['name'])

#         # Return the POIs as a list
#         return poi_list

#     except requests.exceptions.RequestException as e:
#         # Log the error and return an empty list
#         print(f"Error fetching POIs for address {address}: {e}")
#         return []

# def add_poi_to_properties(input_csv, output_csv, api_key, start_row=0, end_row=None):
#     """
#     Adds Points of Interest (POI) data to the properties in the input CSV and saves the extended data to output CSV.
    
#     Parameters:
#     - input_csv (str): Path to the input CSV file containing property data.
#     - output_csv (str): Path to the output CSV file where extended data will be saved.
#     - api_key (str): Google Places API key.
#     - start_row (int): The starting index of the rows to process (default is 0).
#     - end_row (int): The ending index of the rows to process (default is None, which means till the last row).
#     """
#     # Load the scraped property data from CSV
#     properties = pd.read_csv(input_csv)
    
#     # Slice the dataframe to select the rows to process
#     properties_to_process = properties.iloc[start_row:end_row] if end_row else properties.iloc[start_row:]
    
#     # Add a new column for POI data
#     properties_to_process['Nearby_POIs'] = ''
    
#     # Add POI data with retries and delay between requests
#     def get_poi_with_retry(address, lat=None, lng=None):
#         retries = 3
#         for _ in range(retries):
#             pois = get_poi_for_address(address, api_key, lat, lng)
#             if pois:
#                 return pois
#             time.sleep(2)  # Wait before retrying
#         return []  # After retries, return an empty list if all attempts fail

#     # Apply the POI fetching function to each row
#     for idx, row in properties_to_process.iterrows():
#         property_id = row.get('property_id', 'unknown')
        
#         if 'latitude' in row and 'longitude' in row and row['latitude'] and row['longitude']:
#             lat = float(row['latitude'])
#             lng = float(row['longitude'])
#             address = get_address_from_row(row)
            
#             print(f"Processing: {address} (ID: {property_id})")
            
#             # Get POIs for the property
#             pois = get_poi_with_retry(address, lat, lng)
            
#             # Join the list of POIs into a single comma-separated string
#             properties_to_process.at[idx, 'Nearby_POIs'] = ', '.join(pois) if pois else "No POIs found"
#         else:
#             print(f"Skipping: No lat/lng for property ID: {property_id}")
    
#     # Save the extended data to a new CSV
#     properties_to_process.to_csv(output_csv, index=False)
#     print(f"POIs added. Data saved to {output_csv}")

# def get_address_from_row(row):
#     """Extract the complete address from a CSV row"""
#     address_parts = []
    
#     # Ensure all fields are converted to strings if they're not already
#     if 'street' in row and row['street']:
#         address_parts.append(str(row['street']))
#     if 'unit' in row and row['unit']:
#         address_parts.append(str(row['unit']))
#     if 'city' in row and row['city']:
#         address_parts.append(str(row['city']))
#     if 'state' in row and row['state']:
#         address_parts.append(str(row['state']))
#     if 'zip_code' in row and row['zip_code']:
#         address_parts.append(str(row['zip_code']))
    
#     return ", ".join(address_parts) if address_parts else None

# # Usage

# api_key = ''
# input_csv = '/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties/output/properties_raw.csv'
# output_csv = '/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties/output/properties_with_poi.csv'

# # Specify the row slice (for example, rows 0-1000)
# start_row = 0
# end_row = 10

# add_poi_to_properties(input_csv=input_csv, output_csv=output_csv, api_key=api_key, start_row=start_row, end_row=end_row)
