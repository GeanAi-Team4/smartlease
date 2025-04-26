

# smartlease
Smartlease: searching property is now easy.

**Overview:** The AI-Powered Apartment Search is an platform to transform the way people search a property.
Redefines apartment searching by enabling users to search apartments conversationally, bypassing traditional filters and dropdowns, ensuring precise matches

**Team: **Shubham Agarwal, Tirth Desai, Aqeel Ryan
**Course: **DAMG 7275 GenAI LLM in Data Engineering
**Instructor:** Prof. Kishore Aradhya
**Teaching Assistant:** Adwaith Korapati

**Demo: ** 



**Architecture:**
![streamlit_architecture](https://github.com/user-attachments/assets/ec79b88e-e2bb-41f4-a721-14d98d38c10b)

**Features and Pipelines:**

**Pipeline 1 -**
The first pipeline contains a scrape code to fetch data from realtor.com. From the scraped data, the address is extracted and using this address, a set of predefined POIs is fetched using Google Maps Places API. This data is then added with embeddings and stored in the Snowflake Warehouse database.

**Pipeline 2 - S**econd pipeline is a manual data entry feature which contains Fastapi form which takes in the Metadata and embeddings are generated from this Metadata. This data is then stored in the Snowflake Warehouse Database.

**Hybrid Search Pipeline - **This pipeline is the most essential working functionality of SmartLease. User-inputted Natural Language prompt is broken into keywords and preferences by Deep Seek LLM and returned as SQL queries, where identified keywords will come under {where_clause} and Semantic words will be identified as {semantic_clause}. The Semantic words will be used in Cortex to perform a semantic search on ‘complete_property_detail’ column of the dataset. The generated SQL query is used for fetching data from Snowflake warehouse, the responses are then sent to an LLM again to analyse PROs and CONs of the returned property.

**Tech Stack:**
**Backend: **Python,SQL
**Database: **Snowflake Warehouse
**AI tools: **OpenAI, Snowflake Cortex, DeepSeek LLM
**Middleware: **FastAPI
