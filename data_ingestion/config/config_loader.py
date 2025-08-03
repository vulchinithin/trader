import yaml
import os

def load_selection_config():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_dir, 'selection_config.yaml')
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        # Basic validation
        if config['selection']['mode'] not in ['manual', 'autonomous']:
            raise ValueError("Invalid mode in config")
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Selection config not found at {config_path}")
