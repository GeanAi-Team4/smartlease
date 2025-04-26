import openai
import toml
from pathlib import Path
import json

# Load API key
config = toml.load(Path(__file__).parent / "config.toml")
openai.api_key = config["openai"]["api_key"]

def rerank_with_llm(user_query: str, properties: list) -> dict:
    # Limit to top 6 properties
    top_properties = properties[:6]

    # Keep only relevant fields to reduce token usage
    allowed_keys = [
        "property_id", "address", "beds", "full_baths", "sqft", "list_price",
        "nearby_schools", "neighborhoods", "complete_property_details"
    ]

    stripped_properties = [
        {k: v for k, v in prop.items() if k in allowed_keys}
        for prop in top_properties
    ]

    # Construct the prompt
    prompt = f"""
You are a real estate assistant helping a user choose a rental property based on their search query.

Query: "{user_query}"

You are given up to 6 candidate properties. For each:
- RANK the properties based on the user's intent
- For each property, provide:
  - 2 pros
  - 2 cons
  - A 1-line suggestion

Here are the properties:
{json.dumps(stripped_properties, indent=2)}

Return a JSON object in this format:
{{
  "ranked_properties": [
    {{
      "property_id": "...",
      "pros": ["...", "..."],
      "cons": ["...", "...", "..."],
      "suggestion": "..."
    }},
    ...
  ]
}}
    """

    # Send to OpenAI
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    content = response["choices"][0]["message"]["content"]

    # Try parsing the JSON response
    try:
        return json.loads(content)
    except Exception as e:
        return {
            "error": "Failed to parse GPT output",
            "raw_response": content,
            "exception": str(e)
        }
