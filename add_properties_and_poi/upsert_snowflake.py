import pandas as pd
import toml
import snowflake.connector

# Load config
config = toml.load("config.toml")
sf_creds = config['snowflake']
input_csv = config['paths_step_4']['input_csv']

# Snowflake credentials
SNOWFLAKE_ACCOUNT = sf_creds["account"]
SNOWFLAKE_USER = sf_creds["user"]
SNOWFLAKE_PASSWORD = sf_creds["password"]
SNOWFLAKE_ROLE = sf_creds["role"]
SNOWFLAKE_DATABASE = sf_creds["database"]
SNOWFLAKE_SCHEMA = sf_creds["schema"]
SNOWFLAKE_WAREHOUSE = sf_creds["warehouse"]
SNOWFLAKE_TABLE = "PROPERTIES_DATA_WITH_EMBEDDINGS"

def upsert_to_snowflake(properties):
    try:
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

        # Create the table with quoted identifiers to preserve exact names
        column_defs = ', '.join([f'"{col}" STRING' for col in properties.columns])
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_TABLE}" ({column_defs})'
        cursor.execute(create_stmt)

        # Fetch existing property_ids (quoted column name)
        cursor.execute(f'SELECT "property_id" FROM "{SNOWFLAKE_TABLE}"')
        existing_ids = {row[0] for row in cursor.fetchall()}

        new_rows = 0
        for _, row in properties.iterrows():
            property_id = str(row["property_id"])
            if property_id in existing_ids:
                print(f"Skipping property_id {property_id} (already exists)")
                continue

            # Insert using quoted column names
            columns = ', '.join([f'"{col}"' for col in properties.columns])
            placeholders = ', '.join(['%s'] * len(properties.columns))
            insert_sql = f'INSERT INTO "{SNOWFLAKE_TABLE}" ({columns}) VALUES ({placeholders})'
            cursor.execute(insert_sql, tuple(str(row[col]) for col in properties.columns))
            new_rows += 1

        print(f"Inserted {new_rows} new properties.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error while upserting to Snowflake: {e}")

if __name__ == "__main__":
    properties = pd.read_csv(input_csv)
    upsert_to_snowflake(properties)
