# Kafka Spark Ingestion

A production-ready Python package for ingesting data from Kafka Confluent topics into Spark, with Schema Registry support.

## Features

- **Configuration-Driven**: All parameters defined in YAML.
- **Schema Registry Integration**: Automatic schema retrieval and caching.
- **Data Deserialization**: Supports Avro and JSON formats.
- **Validation**: Checks topic existence and schema availability before execution.
- **Flexible Output**: Supports all Spark output sinks (Delta, Parquet, Console, etc.).

## Installation

```bash
pip install kafka_spark_ingestion-1.0.0-py3-none-any.whl
```

## Usage

### 1. Create Configuration

Create a YAML file (e.g., `pipeline_config.yml`):

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "user-events"
  consumer_group: "spark-ingestor"

schema_registry:
  url: "http://localhost:8081"

spark:
  app_name: "UserEventsIngestion"
  streaming: true

output:
  format: "console"
  mode: "append"
  checkpoint_location: "/tmp/checkpoints/user-events"
```

### 2. Run Pipeline

```python
from kafka_spark_ingestion import KafkaSparkPipeline

# Initialize
pipeline = KafkaSparkPipeline("pipeline_config.yml")

# Validate and Run
if pipeline.validate():
    pipeline.run()
else:
    print("Validation failed. Check logs.")
```

### 3. CLI Usage

```bash
python -m kafka_spark_ingestion.pipeline pipeline_config.yml
```

## Development

### Build

```bash
python setup.py bdist_wheel
```

### Test

```bash
python -m unittest discover tests
```
