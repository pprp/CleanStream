import json
import yaml
from modifiers import *

# Function to load the configuration from c4.yaml
def load_config(config_file):
    with open(config_file, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# Function to dynamically apply filters as per the config
def apply_filters(record, filters):
    for filter_func, args in filters.items():
        # Get the function from modifiers module
        func = globals()[filter_func]
        # Assuming the args are given as a list
        if not func(record, *args):
            return False
    return True

# Function to read, parse, and filter the JSONL file using the provided config
def process_jsonl_file(filename, filters):
    filtered_records = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            record = json.loads(line.strip())
            if apply_filters(record, filters):
                filtered_records.append(record)
    return filtered_records

# Specify the filenames
jsonl_filename = './data/top_100.jsonl'
config_filename = 'c4.yaml'

# Load the configuration
config = load_config(config_filename)
# Process the file and get the filtered records based on the config
filtered_records = process_jsonl_file(jsonl_filename, config)

# Print the filtered records
for record in filtered_records:
    print(record)
