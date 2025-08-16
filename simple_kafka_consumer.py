import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

KAFKA_TOPIC = 'github-events'
KAFKA_BROKER_URL = 'localhost:9092'
CONSUMER_GROUP_ID = 'beam-final-consumer-group'


def print_row(row):
    value = row[1].decode()
    print("Printing row's value: " + value)


def run():
    """Defines and runs the Apache Beam pipeline."""
    options = PipelineOptions(streaming=True)

    print("Starting Apache Beam Kafka Consumer Pipeline...")
    print(f"Reading from topic '{KAFKA_TOPIC}' at '{KAFKA_BROKER_URL}'")
    print("Pipeline is running... (Press Ctrl+C to stop)")

    with beam.Pipeline(options=options) as p:
        p | 'Read from Kafka' >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': KAFKA_BROKER_URL,
                'auto.offset.reset': 'latest',
                'group.id': CONSUMER_GROUP_ID
            },
            topics=[KAFKA_TOPIC],
            max_read_time=5
        ) | "print" >> beam.Map(print_row)

    _ = p.run()


if __name__ == '__main__':
    run()