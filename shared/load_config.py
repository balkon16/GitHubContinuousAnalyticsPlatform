import os
import yaml
import logging
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """
    Loads configuration from config.yaml and allows overrides from environment variables.

    The loading hierarchy is as follows (1 overrides 2):
    1. Environment Variable
    2. Value from config.yaml file

    Returns:
        A dictionary containing the application settings.
    """
    # Define fallback defaults in case config.yaml is missing
    defaults = {
        'kafka': {'broker_url': 'localhost:9092', 'topic': 'github-events'},
        'github_api': {'poll_interval_seconds': 60},
        'producer': {'strategy': 'api'}
    }

    # Load base config from YAML file
    try:
        with open('shared/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            if config is None:
                config = defaults
    except FileNotFoundError:
        logging.warning("config.yaml not found! Using default settings.")
        config = defaults
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config.yaml: {e}. Using default settings.")
        config = defaults

    # Helper function to safely get nested dictionary values
    def get_nested(data, keys, default=None):
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return default
        return data if data is not None else default

    # Kafka settings
    kafka_broker_url = os.getenv(
        'KAFKA_BROKER_URL',
        get_nested(config, ['kafka', 'broker_url'], defaults['kafka']['broker_url'])
    )
    kafka_topic = os.getenv(
        'KAFKA_TOPIC',
        get_nested(config, ['kafka', 'topic'], defaults['kafka']['topic'])
    )

    # GitHub API settings
    github_api_url = "https://api.github.com/events"
    poll_interval_seconds = int(os.getenv(
        'POLL_INTERVAL_SECONDS',
        get_nested(config, ['github_api', 'poll_interval_seconds'], defaults['github_api']['poll_interval_seconds'])
    ))

    # Producer settings
    producer_strategy = os.getenv(
        'PRODUCER_STRATEGY',
        get_nested(config, ['producer', 'strategy'], defaults['producer']['strategy'])
    )

    # Assemble and return the final configuration
    return {
        "KAFKA_BROKER_URL": kafka_broker_url,
        "KAFKA_TOPIC": kafka_topic,
        "GITHUB_API_URL": github_api_url,
        "POLL_INTERVAL_SECONDS": poll_interval_seconds,
        "PRODUCER_STRATEGY": producer_strategy,
    }


settings = load_config()

logging.info(f"Configuration loaded: {settings}")
