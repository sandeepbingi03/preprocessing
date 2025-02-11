# preprocessing
import os
import json
import csv
from collections import OrderedDict

# Folder paths
CSV_FOLDER = "csv_input"
JSONL_FOLDER = "jsonl_output"
PROCESSED_LOG = "processed_files.txt"

# Keys to remove, rename, and order
keys_to_remove = {...}  # Same as provided in your script
key_rename = {...}  # Same as provided in your script
key_order = [...]  # Same as provided in your script

def get_processed_files():
    """Retrieve the list of already processed files."""
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            return set(f.read().splitlines())
    return set()

def log_processed_file(filename):
    """Log a processed file."""
    with open(PROCESSED_LOG, "a") as f:
        f.write(filename + "\n")

def convert_csv_to_jsonl(csv_file, jsonl_file):
    """Convert CSV file to JSONL format."""
    with open(csv_file, "r", encoding="utf-8") as infile, open(jsonl_file, "w", encoding="utf-8") as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            json.dump(row, outfile)
            outfile.write("\n")

def preprocess_jsonl(input_file, output_file):
    """Process JSONL data by cleaning and structuring it."""
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            try:
                data = json.loads(line)
                data.pop("preview", None)
                data.pop("lastrow", None)

                if "result" in data:
                    for key in keys_to_remove:
                        data["result"].pop(key, None)
                    data = data["result"]

                # Process raw data
                raw_key = next((key for key in data.keys() if key.endswith("_raw")), None)
                if raw_key:
                    try:
                        raw_data = json.loads(data[raw_key])
                    except json.JSONDecodeError:
                        raw_data = data[raw_key]

                    if isinstance(raw_data, dict) and "msg" in raw_data:
                        if isinstance(raw_data["msg"], dict) and "exception" in raw_data["msg"]:
                            data["msg.exception"] = raw_data["msg"]["exception"]
                        elif isinstance(raw_data["msg"], str):
                            data["msg.exception"] = raw_data["msg"]

                        if "msg.message" not in data or not data["msg.message"]:
                            if isinstance(raw_data["msg"], dict) and "message" in raw_data["msg"]:
                                data["msg.message"] = raw_data["msg"]["message"]
                            elif isinstance(raw_data["msg"], str):
                                data["msg.message"] = raw_data["msg"]

                    data.pop(raw_key, None)

                # Rename keys
                for old_key, new_key in key_rename.items():
                    if old_key in data:
                        data[new_key] = data.pop(old_key)

                # Replace empty values with "N/A"
                for key, value in data.items():
                    if value in ["", "?", None]:
                        data[key] = "N/A"

                # Reorder keys
                ordered_data = OrderedDict()
                for key in key_order:
                    ordered_data[key] = data.get(key, "N/A")

                for key, value in data.items():
                    if key not in ordered_data:
                        ordered_data[key] = value

                # Write to output file
                outfile.write(json.dumps(ordered_data) + "\n")

            except json.JSONDecodeError:
                continue

def process_folder():
    """Convert CSVs to JSONL, then preprocess the JSONL files."""
    processed_files = get_processed_files()

    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv") and filename not in processed_files:
            csv_path = os.path.join(CSV_FOLDER, filename)
            jsonl_path = os.path.join(JSONL_FOLDER, filename.replace(".csv", ".jsonl"))
            output_path = os.path.join(JSONL_FOLDER, filename.replace(".csv", "_processed.jsonl"))

            # Convert CSV to JSONL
            convert_csv_to_jsonl(csv_path, jsonl_path)

            # Preprocess JSONL
            preprocess_jsonl(jsonl_path, output_path)

            # Log processed file
            log_processed_file(filename)

if __name__ == "__main__":
    os.makedirs(JSONL_FOLDER, exist_ok=True)
    process_folder()
