import openai
import toml
from pathlib import Path
import json

config = toml.load(Path(__file__).parent / "config.toml")
openai.api_key = config["openai"]["api_key"]

ALLOWED_FIELDS = [
    "address", "beds", "full_baths", "sqft", "list_price", "neighborhoods", "nearby_schools"
]

def extract_metadata(query: str) -> dict:
    prompt = f"""
    Extract the following fields from the real estate search query below. 
    Only return the fields that are mentioned, in JSON format.

    Fields: {ALLOWED_FIELDS}
    Query: "{query}"
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = response['choices'][0]['message']['content']
    try:
        raw = json.loads(content)
        return {k: v for k, v in raw.items() if k in ALLOWED_FIELDS}
    except:
        return {"error": "Could not parse response", "raw_content": content}
