import json
import yaml
from mappers.factory_utils import is_factory
from mappers.modifiers import *
from mappers.filters.content_filters import *
from mappers.enrichers.language_id_enrichers import * 
from mappers.filters.metadata_filters import *

# Function to load the configuration from c4.yaml
def load_config(config_file):
    with open(config_file, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# Function to dynamically apply filters as per the config
def apply_filters(record, filters):
    if isinstance(record, (list, tuple)):
        record = record[0]
    
    for filter_def in filters:
        print(filter_def, '**')
        if not filter_def:
            continue 
        print(filter_def, "&&")
        if 'func' not in filter_def:
            raise KeyError(f"Missing 'func' key in filter definition: {filter_def}")
        filter_func = filter_def.pop('func')
        args = filter_def  # Remaining items are arguments
        # Get the function from the modifiers module
        func = globals().get(filter_func)
        if not func:
            raise ValueError(f"Function '{filter_func}' not found in globals.")
        
        if is_factory(func):
            filter_instance = func(**args)
            record = filter_instance(record)
        else:
            # If it's a simple function, apply it directly
            record = func(record, **args)
        
        print('--'*10)
        print(record)
        print('--'*10)
        
        if isinstance(record, (list, tuple)):
            print('===', type(record))
            record = record[0]
    
    return record
        
# Function to read, parse, and filter the JSONL file using the provided config
def process_jsonl_file(filename, filters):
    filtered_records = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            record = json.loads(line.strip())
            record = apply_filters(record, filters)
            filtered_records.append(record)
    return filtered_records

# Specify the filenames
jsonl_filename = './data/train-00000-of-00041.jsonl'
config_filename = 'c4.yaml'

# Load the configuration
config = load_config(config_filename)

# Extract filters from the configuration (removing unnecessary entries)
filters = config[0]['steps']

# Process the file and get the filtered records based on the config
filtered_records = process_jsonl_file(jsonl_filename, filters)

# save to json file
SAVE_PATH='./data/saved_train-00000-of-00041.jsonl'

def save_to_jsonl(data, path):
    with open(path, 'w') as file:
        for entry in data:
            json_line = json.dumps(entry)
            file.write(json_line + '\n')

save_to_jsonl(filtered_records, SAVE_PATH)
