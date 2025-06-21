import logging
import random
import uuid
from datetime import datetime, timezone
from typing import List, Dict

from producer.modules.strategy import EventGeneratorStrategy


class SyntheticEventGenerator(EventGeneratorStrategy):
    """Strategy to generate fake events locally for debugging."""

    # TODO: the following events should resemble (in terms of schema) the ones we get from the API
    def __init__(self):
        self._event_templates = [
            {
                "type": "PushEvent",
                "actor": {"login": "synthetic-user"},
                "repo": {"name": "synthetic/repo"},
                "payload": {"size": random.randint(1, 5)}
            },
            {
                "type": "WatchEvent",
                "actor": {"login": "debugger-dave"},
                "repo": {"name": "test/project"},
                "payload": {"action": "started"}
            },
            {
                "type": "PullRequestEvent",
                "actor": {"login": "synthetic-user"},
                "repo": {"name": "another/repo"},
                "payload": {"action": "opened", "number": random.randint(100, 200)}
            }
        ]
        logging.info("[SyntheticStrategy] Initialized.")

    def get_events(self) -> List[Dict]:
        """Generates a small, random batch of synthetic events."""
        num_events = random.randint(1, 3)
        generated_events = []
        for _ in range(num_events):
            template = random.choice(self._event_templates).copy()
            template['id'] = str(uuid.uuid4())
            template['created_at'] = datetime.now(timezone.utc).isoformat()
            generated_events.append(template)

        logging.info(f"[SyntheticStrategy] Generated {len(generated_events)} new events.")
        return generated_events
