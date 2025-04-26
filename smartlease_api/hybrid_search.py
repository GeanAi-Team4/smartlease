import snowflake.connector
import toml
from pathlib import Path
import re

# Load config
config = toml.load(Path(__file__).parent / "config.toml")
sf_creds = config["snowflake"]

# Extract creds
SNOWFLAKE_ACCOUNT = sf_creds["account"]
SNOWFLAKE_USER = sf_creds["user"]
SNOWFLAKE_PASSWORD = sf_creds["password"]
SNOWFLAKE_ROLE = sf_creds["role"]
SNOWFLAKE_DATABASE = sf_creds["database"]
SNOWFLAKE_SCHEMA = sf_creds["schema"]
SNOWFLAKE_WAREHOUSE = sf_creds["warehouse"]

def clean_numeric(value):
    try:
        return float(re.findall(r"[\d.]+", str(value))[0])
    except (IndexError, ValueError):
        return None

def run_hybrid_search(user_query: str, metadata: dict) -> list:
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

    user_query_safe = user_query.replace("'", "''")
    embedding_call = f"SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', '{user_query_safe}')"

    # ---- Semantic Search ----
    semantic_sql = f"""
    SELECT *, 
        VECTOR_COSINE_SIMILARITY(complete_property_details_embedding, {embedding_call}) AS similarity,
        0 AS keyword_score
    FROM properties_data_with_embeddings
    ORDER BY similarity DESC
    LIMIT 20;
    """
    cursor.execute(semantic_sql)
    sem_rows = cursor.fetchall()
    sem_cols = [col[0] for col in cursor.description]
    semantic_results = [dict(zip(sem_cols, row)) for row in sem_rows]

    # ---- Keyword Filtered Search ----
    filter_clauses = []
    for key, value in metadata.items():
        if not value:
            continue

        column = f'"{key.lower()}"'
        num_val = clean_numeric(value)

        if isinstance(value, str) and value.strip().startswith(("<", ">")) and num_val is not None:
            filter_clauses.append(
                f"TRY_TO_NUMBER(REGEXP_REPLACE({column}, '[^0-9.]', '')) {'<' if value.startswith('<') else '>'} {num_val}"
            )
        elif num_val is not None:
            filter_clauses.append(
                f"TRY_TO_NUMBER(REGEXP_REPLACE({column}, '[^0-9.]', '')) = {num_val}"
            )
        else:
            filter_clauses.append(f"{column} ILIKE '%{value}%'")

    where_clause = " AND ".join(filter_clauses)
    where_sql = f"WHERE {where_clause}" if where_clause else ""

    keyword_sql = f"""
    SELECT *, 
        VECTOR_COSINE_SIMILARITY(complete_property_details_embedding, {embedding_call}) AS similarity,
        1 AS keyword_score
    FROM properties_data_with_embeddings
    {where_sql}
    ORDER BY similarity DESC
    LIMIT 20;
    """
    cursor.execute(keyword_sql)
    kw_rows = cursor.fetchall()
    kw_cols = [col[0] for col in cursor.description]
    keyword_results = [dict(zip(kw_cols, row)) for row in kw_rows]

    # ---- Merge, de-dupe, prioritize keyword matches ----
    seen = set()
    combined = []

    for row in keyword_results + semantic_results:
        pid = row.get("property_id")
        if pid and pid not in seen:
            seen.add(pid)
            combined.append(row)

    # Score boost if it came from keyword set (safe access)
    for row in combined:
        similarity = row.get("similarity", 0.0)
        keyword_score = 0.1 if row.get("keyword_score") else 0.0
        row['final_score'] = similarity + keyword_score

    # Sort and take top 6
    top_results = sorted(combined, key=lambda x: x.get('final_score', 0), reverse=True)[:6]

    cursor.close()
    conn.close()
    return top_results
