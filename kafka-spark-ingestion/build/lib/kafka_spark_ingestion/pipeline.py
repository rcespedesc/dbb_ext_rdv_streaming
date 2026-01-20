from pyspark.sql import SparkSession
import logging
import sys
from typing import Optional

from .config_loader import ConfigLoader
from .utils import setup_logger
from .schema_handler import SchemaHandler
from .kafka_reader import KafkaReader
from .transformers import DataTransformer

class KafkaSparkPipeline:
    """
    Main orchestrator for the Kafka to Spark ingestion pipeline.
    """

    def __init__(self, config_path: str):
        """
        Initialize pipeline with YAML config path.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.config
        
        # Setup logging
        log_level = self.config.get("logging", {}).get("level", "INFO")
        self.logger = setup_logger(level=log_level)
        
        self.spark: Optional[SparkSession] = None
        self.schema_handler: Optional[SchemaHandler] = None
        self.kafka_reader: Optional[KafkaReader] = None
        self.transformer: Optional[DataTransformer] = None

    def _init_spark(self):
        """Initializes the Spark Session lazily."""
        if self.spark is None:
            app_name = self.config['spark']['app_name']
            builder = SparkSession.builder.appName(app_name)
            
            # Add extra config if provided
            for key, val in self.config['spark'].get('config', {}).items():
                builder.config(key, val)
                
            self.spark = builder.getOrCreate()
            self.logger.info(f"Spark Session '{app_name}' initialized.")

    def validate(self) -> bool:
        """
        Validate Kafka topic and Schema Registry connectivity.
        
        Returns:
            bool: True if all validations pass.
        """
        self.logger.info("Starting pipeline validation...")
        
        # 1. Validate Config Structure
        try:
            self.config_loader.validate()
        except ValueError as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False

        # 2. Validate Schema Registry
        self.schema_handler = SchemaHandler(self.config['schema_registry'], self.logger)
        if not self.schema_handler.validate_registry_connection():
            return False
            
        # 3. Validate Schema Availability for Topic
        topic = self.config['kafka']['topic']
        schema_str = self.schema_handler.get_schema(topic)
        if not schema_str:
            self.logger.error(f"No schema found for topic '{topic}'. Expected subject '{topic}-value'.")
            return False
            
        # 4. Validate Kafka Topic (Requires Spark or AdminClient)
        # We use AdminClient inside KafkaReader, but we need Spark for KafkaReader init? 
        # Actually KafkaReader uses AdminClient which doesn't need Spark.
        # But our KafkaReader class takes Spark in init. Let's refactor or just init Spark for validation if needed.
        # Or better, instantiate KafkaReader with None for Spark if we just want to validate topic.
        
        # Let's just use AdminClient directly here or make KafkaReader more flexible.
        # For now, we'll init Spark as it's usually cheap enough in local/client mode, 
        # or we can defer Spark init in KafkaReader.
        
        # Let's init Spark for validation to be safe and consistent
        self._init_spark()
        self.kafka_reader = KafkaReader(self.spark, self.config['kafka'], self.logger)
        
        if not self.kafka_reader.validate_topic():
            return False
            
        self.logger.info("Pipeline validation successful.")
        return True

    def run(self):
        """Execute the pipeline."""
        self.logger.info("Starting pipeline execution...")
        
        if not self.spark:
            self._init_spark()
            
        if not self.kafka_reader:
            self.kafka_reader = KafkaReader(self.spark, self.config['kafka'], self.logger)
            
        if not self.schema_handler:
            self.schema_handler = SchemaHandler(self.config['schema_registry'], self.logger)

        # 1. Get Schema
        topic = self.config['kafka']['topic']
        schema_str = self.schema_handler.get_schema(topic)
        if not schema_str:
            raise RuntimeError(f"Schema not found for topic {topic}")

        # 2. Initialize Transformer
        # Assuming Avro for now, could be configurable
        schema_type = self.config.get('schema_registry', {}).get('type', 'AVRO')
        self.transformer = DataTransformer(schema_str, schema_type, self.logger)

        # 3. Read Data
        is_streaming = self.config.get('spark', {}).get('streaming', True)
        if is_streaming:
            df = self.kafka_reader.read_stream()
        else:
            df = self.kafka_reader.read_batch()

        # 4. Transform
        processed_df = self.transformer.transform(df)

        # 5. Write Output
        output_config = self.config['output']
        format = output_config['format']
        mode = output_config['mode']
        
        writer = processed_df.writeStream if is_streaming else processed_df.write
        
        writer = writer.format(format).outputMode(mode)
        
        if 'checkpoint_location' in output_config:
            writer = writer.option("checkpointLocation", output_config['checkpoint_location'])
            
        # Add other options
        for k, v in output_config.get('options', {}).items():
            writer = writer.option(k, v)
            
        self.logger.info(f"Writing data to {format} in {mode} mode...")
        
        if is_streaming:
            query = writer.start()
            query.awaitTermination()
        else:
            writer.save()
            self.logger.info("Batch processing completed.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m kafka_spark_ingestion.pipeline <config_path>")
        sys.exit(1)
    
    pipeline = KafkaSparkPipeline(sys.argv[1])
    if pipeline.validate():
        pipeline.run()
    else:
        sys.exit(1)
