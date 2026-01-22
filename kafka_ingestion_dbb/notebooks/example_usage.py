# Databricks notebook source
# MAGIC %md
# MAGIC # Ejemplo de Uso: Kafka Ingestion DBB

# COMMAND ----------

# MAGIC %pip install kafka_ingestion_dbb-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Crear archivo de configuración de ejemplo
config_yaml = """
source:
  SBB: "demo-ingestion"
  SBB_v: "1.0.0"
  type: "kafka"

source_confluent_cloud:
  PRM_LHCL_AMBIENTE: "dev"
  PRM_LHCL_INCLUDE_HEADERS: false
  PRM_LHCL_USE_SCHEMA_REGISTRY: true
  PRM_LHCL_CHECKPOINT_LOCATION: "/tmp/checkpoints/demo_kafka"
  PRM_LHCL_APPLICATION: "demo-app"
  PRM_LHCL_INPUT_TOPIC: "demo.topic"
  PRM_LHCL_KAFKA_CONSUMER_GROUP: "demo-group"
  PRM_LHCL_MAX_OFFSETS_PER_TRIGGER: 100
  PRM_LHCL_STARTING_OFFSETS_VALUE: "earliest"
  PRM_LHCL_AVRO_OPTION_MODE: "PERMISSIVE"
  PRM_LHCL_PARTITION_COLUMN_NAME: "processing_timestamp"

target:
  PRM_LHCL_TABLE: "demo_table"
  PRM_LHCL_SCHEMA: "default"
  PRM_LHCL_TABLE_DESCRIPTION: "Tabla de demostración"
"""

dbutils.fs.put("/tmp/demo_config.yml", config_yaml, True)

# COMMAND ----------

from kafka_ingestion import run_pipeline

# Ejecutar el pipeline
query = run_pipeline("/dbfs/tmp/demo_config.yml")

# COMMAND ----------

# Monitorear el stream
import time
time.sleep(60)
if query.isActive:
    print("Stream is active processing data...")
    query.stop()
