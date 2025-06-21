import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

from shared.load_config import settings


class ParseJson(beam.DoFn):
    def process(self, element_bytes, **kwargs):
        """
        Parses a byte string into a Python dictionary.
        If parsing fails, it logs the error and the problematic data
        but does not crash the pipeline.
        :param **kwargs:
        """
        try:
            # First, decode the byte string to a UTF-8 string
            element_str = element_bytes.decode('utf-8')
            # Then, load the string as JSON
            yield json.loads(element_str)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error(f"Failed to parse message. Error: {e}. Data: {element_bytes[:200]}")
            # By not 'yielding' anything here, we effectively filter out the bad record.


def run_consumer(argv=None):
    """
    Defines and runs the Apache Beam consumer pipeline.
    This function is designed to be called from an external script.
    """
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    logging.info("[Consumer] Starting Beam pipeline...")
    logging.info(f"[Consumer] Reading from Kafka topic: '{settings['KAFKA_TOPIC']}'")

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read from Kafka
        kafka_records = (
                p | 'ReadFromKafka' >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': settings['KAFKA_BROKER_URL'],
                # 'auto.offset.reset': 'earliest',
                'group.id': 'github-event-beam-consumer'
            },
            topics=[settings['KAFKA_TOPIC']]
        )

        )
        _ = (
                kafka_records
                | 'LogRawBytes' >> beam.Map(lambda record: logging.info(
            f"[Consumer] Received RAW record from Kafka. Value (first 100 bytes): {record.value[:100]}"))
        )

        # Step 2: Process the raw records
        parsed_events = (
                kafka_records
                | 'ExtractEventValue' >> beam.Map(lambda record: record.value)
                | 'DecodeAndParseJson' >> beam.ParDo(ParseJson())
        )

        # Step 3: Output the results for debugging
        _ = (
                parsed_events
                | 'PrintToConsole' >> beam.Map(lambda event: logging.info(f"[Consumer] Consumed event: {event}"))
        )
