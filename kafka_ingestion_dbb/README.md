# Kafka Ingestion DBB

Paquete Python para ingesta de datos desde Kafka Confluent hacia Delta Lake en Databricks usando Spark Structured Streaming.

## Características

- Validación de configuración YAML
- Integración con Azure Key Vault (vía Databricks Secrets)
- Conexión a Kafka Confluent con SASL
- Integración con Confluent Schema Registry
- Parsing automático de mensajes Avro
- Escritura a Delta Lake con checkpointing
- Manejo de errores configurable (PERMISSIVE/FAILFAST)

## Instalación

1. Construir el wheel:
   ```bash
   python setup.py bdist_wheel
   ```

2. Instalar en Databricks:
   ```python
   %pip install /path/to/kafka_ingestion_dbb-1.0.0-py3-none-any.whl
   ```

## Uso

### Configuración YAML

Crear un archivo YAML con la siguiente estructura:

```yaml
source:
  SBB: "ingesta-eventos"
  SBB_v: "1.0.0"
  type: "kafka"

source_confluent_cloud:
  PRM_LHCL_AMBIENTE: "dev"
  PRM_LHCL_INCLUDE_HEADERS: false
  PRM_LHCL_USE_SCHEMA_REGISTRY: true
  PRM_LHCL_CHECKPOINT_LOCATION: "/mnt/datalake/checkpoints/mi_tabla"
  PRM_LHCL_APPLICATION: "mi-app"
  PRM_LHCL_INPUT_TOPIC: "mi.topico.v1"
  PRM_LHCL_KAFKA_CONSUMER_GROUP: "mi-consumer-group"
  PRM_LHCL_MAX_OFFSETS_PER_TRIGGER: 10000
  PRM_LHCL_STARTING_OFFSETS_VALUE: "earliest"
  PRM_LHCL_AVRO_OPTION_MODE: "PERMISSIVE"
  PRM_LHCL_PARTITION_COLUMN_NAME: "processing_date"

target:
  PRM_LHCL_TABLE: "mi_tabla"
  PRM_LHCL_SCHEMA: "mi_schema"
  PRM_LHCL_TABLE_DESCRIPTION: "Tabla de eventos"
```

### Ejecución en Notebook

```python
from kafka_ingestion import run_pipeline

# Ejecutar pipeline
query = run_pipeline("/dbfs/configs/mi_config.yml")

# Esperar terminación (opcional si se ejecuta como job)
# query.awaitTermination()
```

## Secretos

El paquete espera los siguientes secretos en el scope `kv-<ambiente>-scope`:

- `kafka-bootstrap-servers`
- `kafka-security-protocol`
- `kafka-sasl-mechanism`
- `kafka-sasl-jaas-config`
- `schema-registry-url`
- `basic-auth-credentials-source`
- `basic-auth-user-info`
