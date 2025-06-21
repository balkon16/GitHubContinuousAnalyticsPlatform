import os
import time
import json
import logging
from kafka import KafkaProducer

from producer.modules.api_event_generator import ApiEventGenerator
from producer.modules.synthetic_event_generator import SyntheticEventGenerator
from shared.load_config import settings
from producer.modules.strategy import EventGeneratorStrategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_github_token():
    """Retrieves GitHub token from environment variables."""
    token = os.getenv("GITHUB_TOKEN")
    if not token and settings["PRODUCER_STRATEGY"] == "api":
        logging.warning("GITHUB_TOKEN env var not set. API rate limits will be low.")
    return token


def create_kafka_producer() -> KafkaProducer:
    """Creates and returns a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings["KAFKA_BROKER_URL"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all'
        )
        logging.info(f"Successfully connected to Kafka at {settings['KAFKA_BROKER_URL']}.")
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        raise


def select_strategy() -> EventGeneratorStrategy:
    """Factory function to select and instantiate the correct strategy."""
    strategy_name = settings["PRODUCER_STRATEGY"]
    if strategy_name == "api":
        return ApiEventGenerator(
            api_url=settings["GITHUB_API_URL"],
            github_token=get_github_token()
        )
    elif strategy_name == "synthetic":
        return SyntheticEventGenerator()
    else:
        raise NotImplementedError(f"Unknown producer strategy: {strategy_name}")


def run_producer():
    """Main function to run the producer."""
    producer = create_kafka_producer()
    strategy = select_strategy()

    logging.info(f"Starting producer with strategy: '{settings['PRODUCER_STRATEGY']}'")
    logging.info(f"Publishing to Kafka topic: '{settings['KAFKA_TOPIC']}'")

    try:
        while True:
            events = strategy.get_events()

            if events:
                for event in events:
                    logging.info(f'Event info: {event["created_at"]}, {event["repo"]["name"]}')
                    producer.send(settings["KAFKA_TOPIC"], event)
                producer.flush()

            time.sleep(settings["POLL_INTERVAL_SECONDS"])

    except KeyboardInterrupt:
        logging.info("Producer shutting down.")
    finally:
        # producer.close()
        logging.info("Kafka producer closed.")
