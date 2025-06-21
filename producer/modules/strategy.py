import abc
from typing import List, Dict


class EventGeneratorStrategy(abc.ABC):
    """Abstract base class for event generation strategies."""

    @abc.abstractmethod
    def get_events(self) -> List[Dict]:
        """
        Fetches or generates a list of events.
        This method should contain the core logic of the strategy.
        """
        pass
