import os
import pandas as pd
import requests
import time
import random
import toml
import argparse

# Load config
config = toml.load("config.toml")
API_KEYS = config['google_places']['api_keys']
DEFAULT_POI_TYPES = ['restaurant', 'cafe', 'hospital', 'pharmacy', 'atm', 'bank']

def get_poi_details(poi_id, api_key):
    endpoint = "https://maps.googleapis.com/maps/api/place/details/json"
    url = f"{endpoint}?place_id={poi_id}&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json()
        if 'result' in result:
            place = result['result']
            return {
                'name': place.get('name', 'N/A'),
                'address': place.get('formatted_address', 'N/A'),
                'rating': place.get('rating', 'N/A'),
                'user_ratings_total': place.get('user_ratings_total', 'N/A'),
                'types': place.get('types', []),
                'vicinity': place.get('vicinity', 'N/A')
            }
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching details for POI {poi_id}: {e}")
        return None

def get_poi_for_address(address, api_key, lat=None, lng=None):
    endpoint = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    query = f"location={lat},{lng}&radius=2400" if lat and lng else f"query=near {address}"
    url = f"{endpoint}?{query}&key={api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        results = response.json()
        poi_details = []
        if 'results' in results:
            for result in results['results']:
                poi_id = result['place_id']
                details = get_poi_details(poi_id, api_key)
                if details:
                    poi_details.append(details)
        return poi_details
    except requests.exceptions.RequestException as e:
        print(f"Error fetching POIs for address {address}: {e}")
        return []

def add_poi_to_properties(input_csv, output_csv, start_row=0, end_row=None):
    properties = pd.read_csv(input_csv)
    properties_to_process = properties.iloc[start_row:end_row] if end_row else properties.iloc[start_row:]

    for poi_type in DEFAULT_POI_TYPES:
        properties_to_process[f'{poi_type}_name'] = ''
        properties_to_process[f'{poi_type}_rating'] = ''
        properties_to_process[f'{poi_type}_address'] = ''

    def get_poi_with_retry(address, lat=None, lng=None):
        for _ in range(3):
            api_key = random.choice(API_KEYS)
            pois = get_poi_for_address(address, api_key, lat, lng)
            if pois:
                return pois
            time.sleep(random.uniform(1, 2))
        return []

    for idx, row in properties_to_process.iterrows():
        address = row.get('address', '')
        lat = row.get('latitude', None)
        lng = row.get('longitude', None)
        property_id = row.get('property_id', 'unknown')
        print(f"Processing: {address} (ID: {property_id})")

        if address:
            pois = get_poi_with_retry(address, lat, lng)
            for poi in pois:
                for poi_type in DEFAULT_POI_TYPES:
                    if poi_type in poi['types']:
                        properties_to_process.at[idx, f'{poi_type}_name'] = poi['name']
                        properties_to_process.at[idx, f'{poi_type}_rating'] = poi['rating']
                        properties_to_process.at[idx, f'{poi_type}_address'] = poi['address']

    properties_to_process.to_csv(output_csv, index=False)
    print(f"POIs added. Data saved to {output_csv}")
    return len(properties_to_process), output_csv

# CLI support
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_row", type=int, default=0)
    parser.add_argument("--end_row", type=int, default=None)

    args = parser.parse_args()

    input_csv = config["paths_step_2"]["input_csv"]
    output_csv = config["paths_step_2"]["output_csv"]

    # add_poi_to_properties(input_csv=input_csv, output_csv=output_csv, start_row=args.start_row, end_row=args.end_row)
    # add_poi_to_properties(input_csv=input_csv, output_csv=output_csv, start_row=0, end_row=250)
    add_poi_to_properties(
        input_csv=input_csv,
        output_csv=output_csv,
        start_row=args.start_row,
        end_row=args.end_row
    )



