# GitHub - Continuous Analytics Platform

This project implements a resilient, event-driven data pipeline to perform continuous analytics on the public GitHub event stream. It is designed to handle the unpredictable latency of the GitHub Events API by leveraging event-time processing with Apache Kafka and Apache Beam.

The primary goal is to produce accurate, aggregated analytics (e.g., "hourly trending repositories") despite events arriving late and out-of-order.

## Core Technologies

*   **Data Source**: [GitHub Events API](https://docs.github.com/en/rest/activity/events#list-public-events)
*   **Data Producer**: Python (`requests`, `kafka-python`)
*   **Streaming Bus**: Apache Kafka
*   **Processing Framework**: Apache Beam (Python SDK)
*   **Data Sink**: InfluxDB (for time-series analytics)
*   **Visualization**: Grafana

# Execution

## Broker

```shell
docker-compose up -d
```

Manually send messages to the topic:
```shell
docker exec -it kafka kafka-console-producer --topic github-events --bootstrap-server kafka:29092
```

Manually read messages from the topic:
```shell
docker exec -it kafka kafka-console-consumer --topic github-events --bootstrap-server kafka:29092 --from-beginning
```

## Producer

```bash
export PRODUCER_STRATEGY=api # optional
export GITHUB_TOKEN=<your token>
python main.py
```