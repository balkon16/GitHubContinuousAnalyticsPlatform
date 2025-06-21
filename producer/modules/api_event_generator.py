import logging
from typing import Optional, List, Dict

import requests

from producer.modules.strategy import EventGeneratorStrategy
from producer.modules.state_manager import load_state, save_state

class ApiEventGenerator(EventGeneratorStrategy):
    """Strategy to fetch real events from the GitHub Events API."""

    def __init__(self, api_url: str, github_token: Optional[str]):
        self.api_url = api_url
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if github_token:
            self.headers["Authorization"] = f"token {github_token}"

        # State is managed internally by this strategy
        state = load_state() or {}
        self.last_etag = state.get("etag")
        if self.last_etag:
            self.headers["If-None-Match"] = self.last_etag
        logging.info(f"[ApiStrategy] Initialized. ETag: {self.last_etag}")

    def get_events(self) -> List[Dict]:
        try:
            response = requests.get(self.api_url, headers=self.headers, timeout=10)

            if 'ETag' in response.headers:
                self.last_etag = response.headers['ETag']
                self.headers['If-None-Match'] = self.last_etag

            rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
            if rate_limit_remaining:
                logging.info(f"[ApiStrategy] Rate limit remaining: {rate_limit_remaining}")

            if response.status_code == 200:
                events = response.json()
                save_state({"etag": self.last_etag})  # Persist state on success
                logging.info(f"[ApiStrategy] Fetched {len(events)} new events.") # TODO: can I get more than 30 events per hit?
                return events
            elif response.status_code == 304:
                logging.info("[ApiStrategy] No new events (304 Not Modified).")
                return []
            else:
                logging.error(f"[ApiStrategy] Error: {response.status_code} - {response.text}")
                return []

        except requests.exceptions.RequestException as e:
            logging.error(f"[ApiStrategy] Request failed: {e}")
            return []