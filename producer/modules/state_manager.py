import json
import os
import logging
from typing import Optional, Dict, Any

STATE_FILE = "producer/data/producer_state.json"


def save_state(state: Dict[str, Any]):
    """
    Saves the producer state (e.g., the last ETag) to a JSON file.

    This function overwrites the file with the new state.

    Args:
        state: A dictionary containing the state to be saved.
    """
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
        logging.debug(f"Successfully saved state to {STATE_FILE}")
    except IOError as e:
        logging.error(f"Failed to write state to {STATE_FILE}: {e}")


def load_state() -> Optional[Dict[str, Any]]:
    """
    Loads the producer state from a JSON file.

    This function handles cases where the file doesn't exist (e.g., first run)
    or contains invalid JSON.

    Returns:
        A dictionary containing the loaded state, or None if the file
        does not exist or is invalid.
    """
    if not os.path.exists(STATE_FILE):
        logging.info(f"State file '{STATE_FILE}' not found. Starting with no state.")
        return None

    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            logging.info(f"Successfully loaded state from {STATE_FILE}")
            return state
    except json.JSONDecodeError:
        logging.warning(f"Could not decode JSON from {STATE_FILE}. Starting with no state.")
        return None
    except IOError as e:
        logging.error(f"Failed to read state from {STATE_FILE}: {e}")
        return None
