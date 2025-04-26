import os
import pandas as pd
import toml
import snowflake.connector

# Load config
config = toml.load("config.toml")
sf_creds = config['snowflake']

# Snowflake credentials
SNOWFLAKE_ACCOUNT = sf_creds["account"]
SNOWFLAKE_USER = sf_creds["user"]
SNOWFLAKE_PASSWORD = sf_creds["password"]
SNOWFLAKE_ROLE = sf_creds["role"]
SNOWFLAKE_DATABASE = sf_creds["database"]
SNOWFLAKE_SCHEMA = sf_creds["schema"]
SNOWFLAKE_WAREHOUSE = sf_creds["warehouse"]
SNOWFLAKE_TABLE = "PROPERTY_DATA"

# Path to store uploaded images
IMAGE_UPLOAD_DIR = "/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/add_properties_form/images"
os.makedirs(IMAGE_UPLOAD_DIR, exist_ok=True)

def save_image(file, property_id, index):
    """
    Saves an uploaded image to the image directory.
    """
    if file:
        filename = f"image_{index}_{property_id}.jpg"
        path = os.path.join(IMAGE_UPLOAD_DIR, filename)
        with open(path, "wb") as f:
            f.write(file)
        return path
    return ""

def upsert_single_property(data: dict, image1: bytes = None, image2: bytes = None):
    """
    Upserts a single property into Snowflake.
    """
    try:
        property_id = data.get("property_id")
        primary_photo = save_image(image1, property_id, 1)
        alt_photos = save_image(image2, property_id, 2)

        data["primary_photo"] = primary_photo
        data["alt_photos"] = alt_photos

        # Fill in any missing columns
        all_columns = [
            'property_id','address','status','style','beds','full_baths','sqft','year_built','list_price',
            'latitude','longitude','neighborhoods','county','nearby_schools','primary_photo','alt_photos',
            'restaurant_name','restaurant_rating','restaurant_address','cafe_name','cafe_rating','cafe_address',
            'hospital_name','hospital_rating','hospital_address','pharmacy_name','pharmacy_rating','pharmacy_address',
            'atm_name','atm_rating','atm_address','bank_name','bank_rating','bank_address','complete_property_details'
        ]
        for col in all_columns:
            if col not in data:
                data[col] = ""

        df = pd.DataFrame([data])
        df["complete_property_details"] = df.apply(lambda row: ', '.join(row.astype(str)), axis=1)

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        cursor = conn.cursor()

        # Create table if not exists
        column_defs = ', '.join([f'"{col}" STRING' for col in all_columns])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_TABLE}" ({column_defs})')

        # Check if property_id exists
        cursor.execute(f'SELECT "property_id" FROM "{SNOWFLAKE_TABLE}" WHERE "property_id" = %s', (property_id,))
        if cursor.fetchone():
            return {"status": "duplicate", "message": f"Property ID '{property_id}' already exists."}

        # Insert data
        columns = ', '.join([f'"{col}"' for col in all_columns])
        placeholders = ', '.join(['%s'] * len(all_columns))
        cursor.execute(
            f'INSERT INTO "{SNOWFLAKE_TABLE}" ({columns}) VALUES ({placeholders})',
            tuple(str(df[col].iloc[0]) for col in all_columns)
        )

        conn.commit()
        cursor.close()
        conn.close()

        return {"status": "success", "message": "Property added successfully."}

    except Exception as e:
        return {"status": "error", "message": str(e)}
