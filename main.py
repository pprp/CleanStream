import json
import yaml
from modifiers import *

# Function to load the configuration from c4.yaml
def load_config(config_file):
    with open(config_file, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# Function to dynamically apply filters as per the config
def apply_filters(record, filters):
    for filter_def in filters:
        filter_func = filter_def.pop('func')
        args = filter_def  # Remaining items are arguments
        # Get the function from the modifiers module
        func = globals()[filter_func]
        # Passing the record and unpacking args dictionary
        if not func(record, **args):
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

# Extract filters from the configuration (removing unnecessary entries)
filters = [step for step in config if 'func' in step]

# Process the file and get the filtered records based on the config
filtered_records = process_jsonl_file(jsonl_filename, filters)

# Print the filtered records
for record in filtered_records:
    print(record)
