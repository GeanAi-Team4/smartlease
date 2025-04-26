import streamlit as st
import pandas as pd
import requests
import re
import snowflake.connector
import toml
from pathlib import Path

# ------------------ CONFIG ------------------
config = toml.load(Path(__file__).parent / "smartlease_api" / "config.toml")
sf_creds = config["snowflake"]
base_url = "http://127.0.0.1:8000"

# ------------------ Snowflake Helpers ------------------
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=sf_creds["user"],
        password=sf_creds["password"],
        account=sf_creds["account"],
        warehouse=sf_creds["warehouse"],
        database=sf_creds["database"],
        schema=sf_creds["schema"],
        role=sf_creds["role"]
    )

def fetch_property_details_by_id(property_id):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT "property_id", "address", "status", "style", "beds", "full_baths", "sqft", "year_built", 
               "list_price", "primary_photo", "alt_photos"
        FROM properties_data_with_embeddings
        WHERE "property_id" = %s
    ''', (property_id,))
    row = cursor.fetchone()
    cols = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return dict(zip(cols, row)) if row else {}

# ------------------ Auth Helpers ------------------
def email_valid(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def create_user_table_if_needed():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS USERS (
            EMAIL STRING PRIMARY KEY,
            PASSWORD STRING
        )
    """)
    cursor.close()
    conn.close()

def signup_user(email, password):
    create_user_table_if_needed()
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO USERS (EMAIL, PASSWORD) VALUES (%s, %s)", (email, password))
        return True
    except:
        return False
    finally:
        cursor.close()
        conn.close()

def authenticate_user(email, password):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM USERS WHERE EMAIL = %s AND PASSWORD = %s", (email, password))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result is not None

# ------------------ UI ------------------
st.set_page_config(page_title="SmartLease App", layout="wide")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "email" not in st.session_state:
    st.session_state.email = ""

def show_login_signup():
    # Two-column layout for side-by-side logos
    col1, col2 = st.columns([1, 1])

    with col1:
        st.image("/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/logos/smartlease_logo.jpeg", width=120)
    with col2:
        st.image("/Users/shubhamagarwal/Documents/Northeastern/semester_4/GenAI_LLMs_DE/smartlease/logos/QR_Smartlease_Team_9.jpeg", width=120)
        

    st.title("SmartLease Login")
    choice = st.radio("Choose option:", ["Login", "Signup"], horizontal=True)
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if not email_valid(email):
        st.warning("Enter a valid email.")

    if choice == "Signup" and st.button("Signup"):
        if signup_user(email, password):
            st.success("Signup successful. Please login.")
        else:
            st.error("User already exists.")
    if choice == "Login" and st.button("Login"):
        if authenticate_user(email, password):
            st.session_state.logged_in = True
            st.session_state.email = email
            st.rerun()
        else:
            st.error("Invalid credentials.")

def show_main_ui():
    st.success(f"Welcome, {st.session_state.email} ")
    tab1, tab2, tab3 = st.tabs(["Add new properties and POI", "Add Property manually", "Search Property"])

    # ----------------- Full Pipeline -----------------
    with tab1:
        st.header("Add new properties and POI")
        location = st.text_input("Location", value="Boston")
        listing_type = st.selectbox("Listing Type", ["for_rent", "for_sale"])
        past_days = st.number_input("Past Days", min_value=1, max_value=30, value=3)
        start_row = st.number_input("Start Row", min_value=0, value=0)
        end_row = st.number_input("End Row", min_value=1, value=5)

        if st.button("Run Pipeline"):
            payload = {
                "location": location,
                "listing_type": listing_type,
                "past_days": past_days,
                "start_row": start_row,
                "end_row": end_row
            }
            res = requests.post(f"{base_url}/run-property-pipeline", json=payload)
            if res.status_code == 200:
                st.success(res.json()["message"])
            else:
                st.error("Error running pipeline.")

    # ----------------- Add Property -----------------
    with tab2:
        st.header("Add Property using Form")
        with st.form("add_property_form"):
            property_id = st.text_input("Property ID")
            address = st.text_input("Address")
            status = st.selectbox("Status", ["Active", "Inactive"])
            beds = st.text_input("Beds")
            baths = st.text_input("Baths")
            sqft = st.text_input("Sqft")
            year_built = st.text_input("Year Built")
            list_price = st.text_input("List Price")
            nearby_schools = st.text_input("Nearby Schools")
            primary_photo = st.file_uploader("Primary Photo")
            alt_photo = st.file_uploader("Alternate Photo")
            submitted = st.form_submit_button("Submit")

            if submitted:
                files = {}
                if primary_photo:
                    files["primary_photo"] = (primary_photo.name, primary_photo.read())
                if alt_photo:
                    files["alt_photo"] = (alt_photo.name, alt_photo.read())

                data = {
                    "property_id": property_id,
                    "address": address,
                    "status": status,
                    "beds": beds,
                    "baths": baths,
                    "sqft": sqft,
                    "year_built": year_built,
                    "list_price": list_price,
                    "nearby_schools": nearby_schools
                }

                res = requests.post(f"{base_url}/add-property-form", data=data, files=files)
                if res.status_code == 200:
                    st.success("✅ Property added successfully!")
                else:
                    st.error("❌ Failed to add property.")

    # ----------------- Hybrid Search -----------------
    with tab3:
        st.header("Property Search")
        query = st.text_area("Enter your search query")

        if st.button("Search"):
            res = requests.post(f"{base_url}/hybrid-search/", json={"query": query})
            if res.status_code != 200:
                st.error("Search failed.")
                return

            data = res.json()
            for idx, prop in enumerate(data["ranked_properties"]):
                st.markdown(f"## 🏠 Property #{idx+1}")
                meta = fetch_property_details_by_id(prop["property_id"])

                if meta:
                    left, right = st.columns([1.5, 1.5])

                    with left:
                        st.markdown("### 📍 Property Details")
                        st.markdown(f"- **Address:** {meta.get('address') or 'N/A'}")
                        st.markdown(f"- **Style:** {meta.get('style') or 'N/A'}")
                        st.markdown(f"- **Beds/Baths:** {meta.get('beds') or 'N/A'} / {meta.get('full_baths') or 'N/A'}")
                        st.markdown(f"- **Sqft:** {meta.get('sqft') or 'N/A'}")
                        st.markdown(f"- **Year Built:** {meta.get('year_built') or 'N/A'}")
                        st.markdown(f"- **Price:** ${meta.get('list_price') or 'N/A'}")
                        st.markdown(f"- **Status:** {meta.get('status') or 'N/A'}")

                    with right:
                        image_url = meta.get("primary_photo") or meta.get("alt_photos")
                        if image_url and isinstance(image_url, str) and image_url.strip().lower() != "nan":
                            st.image(image_url, width=400, caption="Property Image")
                        else:
                            st.info("No image available.")

                st.markdown(f"💡 **Suggestion:** {prop['suggestion']}")

                pros_col, cons_col = st.columns(2)
                with pros_col:
                    st.markdown("✅ **Pros**")
                    for p in prop["pros"]:
                        st.markdown(f"- {p}")
                with cons_col:
                    st.markdown("⚠️ **Cons**")
                    for c in prop["cons"]:
                        st.markdown(f"- {c}")

                st.markdown("---")

    # ----------------- Logout -----------------
    st.button("Logout", on_click=lambda: st.session_state.clear())

# ------------------ ENTRY ------------------
if not st.session_state.logged_in:
    show_login_signup()
else:
    show_main_ui()
