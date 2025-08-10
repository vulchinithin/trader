import yaml
import os
from pathlib import Path

def load_config(config_dir=None):
    """
    Load all YAML configuration files from the config directory and merge them.
    Also, load sensitive data from environment variables.
    """
    if config_dir is None:
        config_dir = Path(__file__).parent
    else:
        config_dir = Path(config_dir)

    # Load all YAML files in the directory
    config = {}
    for config_file in config_dir.glob('*.yaml'):
        with open(config_file) as f:
            config_data = yaml.safe_load(f)
            if config_data:
                config.update(config_data)

    # Load secrets from environment variables
    binance_api_key = os.getenv("BINANCE_API_KEY")
    binance_api_secret = os.getenv("BINANCE_API_SECRET")

    if 'binance' not in config:
        config['binance'] = {}

    # Environment variables take precedence
    if binance_api_key:
        config['binance']['api_key'] = binance_api_key
    if binance_api_secret:
        config['binance']['api_secret'] = binance_api_secret

    # For Kafka, we'll get it from the docker-compose or a default
    config.setdefault('kafka', {}).setdefault('bootstrap_servers', os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))

    return config

if __name__ == '__main__':
    # For testing purposes
    config = load_config()
    import json
    print(json.dumps(config, indent=2))
