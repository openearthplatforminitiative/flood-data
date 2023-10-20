import os
import json

def load_config(key, default=None, config_filename='config.json'):
    # Try environment variable first
    value = os.environ.get(key)
    
    if value:
        return value

    # Fall back to external configuration file
    with open(config_filename, 'r') as f:
        config = json.load(f)
        return config.get(key, default)