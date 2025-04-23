import streamlit as st
import openai
import snowflake.connector
import json
import re

# --- CONFIG ---
openai.api_key = st.secrets["OPENAI_API_KEY"]  # put this in .streamlit/secrets.toml

# Helper to extract numeric value from strings like "4 bedrooms" or "$2500" 
def clean_numeric(value):
    try:
        return float(re.findall(r"[\d.]+", str(value))[0])
    except (IndexError, ValueError):
        return None

# --- Metadata extraction ---
def extract_metadata_from_query(user_query: str) -> dict:
    system_prompt = """
You are an intelligent assistant that extracts structured metadata from real estate search queries.

Extract only the fields that are mentioned in the user query. Return the output in JSON format.

Valid metadata fields:
- PROPERTY_ID
- PROPERTY_TITLE
- PROPERTY_ADDRESS
- MONTHLY_RENT
- BEDROOMS
- BATHROOMS
- SQUARE_FEET
- AVAILABILITY
- DEPOSIT
- LEASE_DURATION
- HOUSE_FEATURES
- UTILITIES_INCLUDED
- ABOUT_PROPERTY_DETAILS
- NEIGHBORHOOD_DETAILS
- ADDITIONAL_PROPERTY_DETAILS
- PROPERTY_IMAGE_FOLDER_S3_PATH
"""
    user_prompt = f"User Query: {user_query}\n\nReturn metadata in JSON format."

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2
    )

    raw_content = response["choices"][0]["message"]["content"]
    return json.loads(raw_content)

# --- Hybrid Query ---
def run_hybrid_query(user_query, metadata_filters):
    user_query_safe = user_query.replace("'", "''")

    conn = snowflake.connector.connect(
        account="SFEDU02-PDB57018",
        user="BADGER",
        password="",
        role="TRAINING_ROLE",
        warehouse="LISTINGS_WH",
        database="LISTINGS",
        schema="PUBLIC"
    )
    cur = conn.cursor()

    filter_clauses = []
    for key, value in metadata_filters.items():
        if not value:
            continue

        numeric_value = clean_numeric(value)

        if isinstance(value, str) and value.strip().startswith("<") and numeric_value is not None:
            filter_clauses.append(f"TRY_TO_NUMBER(REGEXP_REPLACE({key}, '[^0-9.]', '')) < {numeric_value}")
        elif isinstance(value, str) and value.strip().startswith(">") and numeric_value is not None:
            filter_clauses.append(f"TRY_TO_NUMBER(REGEXP_REPLACE({key}, '[^0-9.]', '')) > {numeric_value}")
        elif numeric_value is not None:
            filter_clauses.append(f"TRY_TO_NUMBER(REGEXP_REPLACE({key}, '[^0-9.]', '')) = {numeric_value}")
        elif isinstance(value, bool):
            filter_clauses.append(f"{key} = {str(value).upper()}")
        else:
            filter_clauses.append(f"{key} ILIKE '%{value}%'")

    where_clause = " AND ".join(filter_clauses)
    if where_clause:
        where_clause = f"WHERE {where_clause}"

    sql = f"""
    SELECT *,
      VECTOR_COSINE_SIMILARITY(
        PROPERTIES_EMBEDDING,
        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', '{user_query_safe}')
      ) AS similarity
    FROM PROPERTIES_WITH_EMBEDDINGS
    {where_clause}
    ORDER BY similarity DESC
    LIMIT 10;
    """

    try:
        cur.execute(sql)
        results = cur.fetchall()
    except Exception as e:
        st.error(f"Query failed: {e}")
        results = []

    if not results:
        fallback_sql = f"""
        SELECT *,
          VECTOR_COSINE_SIMILARITY(
            PROPERTIES_EMBEDDING,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', '{user_query_safe}')
          ) AS similarity
        FROM PROPERTIES_WITH_EMBEDDINGS
        ORDER BY similarity DESC
        LIMIT 10;
        """
        cur.execute(fallback_sql)
        results = cur.fetchall()

    columns = [col[0] for col in cur.description]
    cur.close()
    conn.close()

    return [dict(zip(columns, row)) for row in results]

# --- STREAMLIT UI ---
st.set_page_config(page_title="SmartLease", page_icon="🏡")
st.title("SmartLease Hybrid Property Search")

query = st.text_input("Enter your property requirements('2-bedroom in Boston under $3000 with parking')")

if query:
    with st.spinner("Extracting metadata from your query..."):
        try:
            metadata = extract_metadata_from_query(query)
            st.success("Metadata extracted!")
            st.json(metadata)  # Optional for debugging
        except Exception as e:
            st.error(f"Metadata extraction failed: {e}")
            st.stop()

    with st.spinner("Searching for matching properties"):
        results = run_hybrid_query(query, metadata)

    st.subheader("Top Matching Properties")

    if results:
        for i, res in enumerate(results, 1):
            with st.container():
                st.markdown(f"### {i}. {res.get('PROPERTY_TITLE', 'Untitled')}")
                st.markdown(f"**Address:** {res.get('PROPERTY_ADDRESS', 'N/A')}")

                col1, col2, col3 = st.columns(3)
                col1.metric("Rent", f"${res.get('MONTHLY_RENT', 'N/A')}")
                col2.metric("Bedrooms", res.get("BEDROOMS", 'N/A'))
                col3.metric("Bathrooms", res.get("BATHROOMS", 'N/A'))

                col4, col5, col6 = st.columns(3)
                col4.metric("Sq Ft", res.get("SQUARE_FEET", 'N/A'))
                col5.metric("Lease", res.get("LEASE_DURATION", 'N/A'))
                col6.metric("Similarity", f"{res.get('similarity', 0):.2f}")

                st.markdown("---")
    else:
        st.warning("No matching properties found.")
