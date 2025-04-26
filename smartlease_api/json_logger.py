import json
from pathlib import Path

# Define the temp_logs directory relative to this file
log_dir = Path(__file__).parent / "temp_logs"
log_dir.mkdir(exist_ok=True)  # Create it if it doesn't exist

def save_step_data(filename: str, data: dict):
    """
    Save a dictionary as a JSON file in the temp_logs folder.
    """
    filepath = log_dir / filename
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def clear_temp_logs():
    """
    Delete all JSON files in the temp_logs folder.
    """
    for file in log_dir.glob("*.json"):
        file.unlink()
