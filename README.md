import os
import json
import csv
from collections import OrderedDict

# Folder paths
CSV_FOLDER = r"C:/Users/Desktop/LLM Logs/test"
JSONL_FOLDER = r"C:/Users/Desktop/LLM Logs/preprocessed/preprocessed_logs_jsonl"
PROCESSED_LOG = r"C:/Users/Desktop/LLM Logs/processed_files.txt"

# Define keys to remove from JSON
keys_to_remove = {
    '_time', 'f_app_id', 'deployment', 'host', 'info_splunk_index', 'linecount',
    'nozzle-event-counter', 'punct', 'splunk_server', 'subscription-id',
    'timestamp', 'origin', 'source', 'sourcetype', 'job', 'job_index',
    'tag: : eventtype', 'msg. threadName', 'source_type', 'eventtype',
    'message_type', 'source_instance', 'event_type', 'index', 'ip', 'uuid',
    'msg. clientId'
}

# Keys to keep in the final JSON output (all other keys will be removed)
key_order = [
    "event_time", "log_level", "channel_type", "app_name", "space_name",
    "spring_profile", "calling_API", "class_name", "method_name",
    "exception", "message", "correlation_id", "transaction_id"
]

# Mapping of old keys to new keys
key_rename = {
    'msg.eventTime': 'event_time',
    'cf_app_name': 'app_name',
    'cf_space_name': 'space_name',
    'msg.channelType': 'channel_type',
    'msg.message': 'message',
    'msg.exception': 'exception',
    'msg.className': 'class_name',
    'msg.correlationId': 'correlation_id',
    'msg.logLevel': 'log_level',
    'msg.springProfile': 'spring_profile',
    'msg.transactionId': 'transaction_id',
    'msg.callingAPI': 'calling_API',
    'msg.methodName': 'method_name'
}

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
    """Convert CSV file to JSONL format, keeping only the '_raw' column."""
    with open(csv_file, "r", encoding="utf-8") as infile, open(jsonl_file, "w", encoding="utf-8") as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            if "_raw" in row and row["_raw"].strip():  # Ensure _raw exists and is not empty
                json.dump({"_raw": row["_raw"]}, outfile)
                outfile.write("\n")

def preprocess_jsonl(input_file, output_file):
    """Process JSONL to extract only key_order keys from '_raw'."""
    with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
        for line in infile:
            try:
                record = json.loads(line)
                if "_raw" not in record:
                    continue  # Skip records without _raw

                try:
                    raw_data = json.loads(record["_raw"])  # Parse _raw as JSON
                except json.JSONDecodeError:
                    continue  # Skip if _raw is not valid JSON

                # Remove unwanted keys
                for key in keys_to_remove:
                    raw_data.pop(key, None)

                # Rename keys
                for old_key, new_key in key_rename.items():
                    if old_key in raw_data:
                        raw_data[new_key] = raw_data.pop(old_key)

                # Strictly filter only key_order keys
                filtered_data = OrderedDict(
                    (key, raw_data.get(key, "N/A")) for key in key_order
                )

                # Write the cleaned JSON object to the output file
                outfile.write(json.dumps(filtered_data) + "\n")

            except json.JSONDecodeError:
                continue  # Skip invalid JSON lines

def process_folder():
    """Convert CSVs to JSONL, then preprocess JSONL files."""
    processed_files = get_processed_files()

    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv") and filename not in processed_files:
            csv_path = os.path.join(CSV_FOLDER, filename)
            jsonl_path = os.path.join(JSONL_FOLDER, filename.replace(".csv", ".jsonl"))
            output_path = os.path.join(JSONL_FOLDER, filename.replace(".csv", "_processed.jsonl"))

            # Convert CSV to JSONL (only _raw column)
            convert_csv_to_jsonl(csv_path, jsonl_path)

            # Preprocess JSONL (extract key_order from _raw)
            preprocess_jsonl(jsonl_path, output_path)

            # Log processed file
            log_processed_file(filename)

if __name__ == "__main__":
    os.makedirs(JSONL_FOLDER, exist_ok=True)
    process_folder()
