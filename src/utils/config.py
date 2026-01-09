# utils/config.py
import json
import os
from typing import Dict, Any

def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from file or return defaults"""
    
    # Default configuration
    default_config = {
        'data_dir': 'data',
        'downloads_dir': 'downloads',
        'log_dir': 'logs',
        'export_to_google_sheets': True,
        'modules': {
            'bkam_forex': {'enabled': True, 'retry_attempts': 3},
            'bkam_treasury': {'enabled': True, 'retry_attempts': 3},
            'investing_masi': {'enabled': True, 'retry_attempts': 3},
            'trading_economics': {'enabled': True, 'retry_attempts': 2},
            'yahoo_markets': {'enabled': True, 'retry_attempts': 3}
        }
    }
    
    # Try to load from config file
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                # Merge with defaults
                default_config.update(file_config)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
    
    # Try to load from environment variables
    env_data_dir = os.getenv('EXPLORER_DATA_DIR')
    if env_data_dir:
        default_config['data_dir'] = env_data_dir
    
    return default_config