from pyspark.sql import SparkSession
import yaml
import sys
from kafka_ingestion.utils.logger import get_logger
from kafka_ingestion.config.validator import ConfigValidator
from kafka_ingestion.security.keyvault_manager import KeyVaultManager
from kafka_ingestion.kafka.connection_validator import KafkaConnectionValidator
from kafka_ingestion.kafka.stream_reader import KafkaStreamReader
from kafka_ingestion.kafka.schema_registry import SchemaRegistryManager
from kafka_ingestion.processing.message_parser import MessageParser

logger = get_logger(__name__)

class KafkaIngestionPipeline:
    def __init__(self, config_path: str, spark: SparkSession = None):
        self.config_path = config_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.config = None
        self.logger = logger
    
    def run(self):
        """Executes the complete pipeline"""
        try:
            self.logger.info(f"Starting pipeline with config: {self.config_path}")
            
            # 1. Load and Validate Config
            self.config = self._load_and_validate_config()
            
            # 2. Setup Key Vault and Get Secrets
            self.logger.info("Setting up Key Vault...")
            kv_manager = self._setup_keyvault()
            kafka_config = kv_manager.get_kafka_config()
            sr_config = kv_manager.get_schema_registry_config()
            
            # 3. Validate Connections
            self.logger.info("Validating connections...")
            self._validate_connections(kafka_config, sr_config)
            
            # 4. Create Stream
            self.logger.info("Creating Kafka stream...")
            stream_df = self._create_kafka_stream(kafka_config)
            
            # 5. Parse Messages
            self.logger.info("Configuring message parsing...")
            parsed_df = self._parse_messages(stream_df, sr_config)
            
            # 6. Write to Delta
            self.logger.info("Starting write to Delta Lake...")
            query = self._write_to_delta(parsed_df)
            
            self.logger.info(f"Stream started. Query ID: {query.id}")
            # We don't await termination here to allow job control from outside, 
            # but typically in a job you might want to await. 
            # For a library, returning the query object is often better.
            # However, if this is the main entry point for a job, we should probably await.
            # Let's return the query object so the caller can decide.
            return query
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            raise
    
    def _load_and_validate_config(self):
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        validator = ConfigValidator()
        validator.validate(config)
        return config
    
    def _setup_keyvault(self):
        # Import DBUtils inside method to avoid import error locally if not mocked
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
        except ImportError:
            # Fallback for local testing if needed, or raise
            self.logger.warning("DBUtils not found. Assuming local test or mocked environment.")
            # If we are strictly in Databricks, this should be there. 
            # For now, let's assume we need it.
            # If we are running locally, we might need a mock.
            # Let's try to get it from spark.conf if possible or just fail if not present.
            # But the prompt implies this is for Databricks.
            raise ImportError("pyspark.dbutils.DBUtils is required but not found.")

        ambiente = self.config['source_confluent_cloud']['PRM_LHCL_AMBIENTE']
        return KeyVaultManager(dbutils, ambiente)
    
    def _validate_connections(self, kafka_config, sr_config):
        validator = KafkaConnectionValidator()
        topic = self.config['source_confluent_cloud']['PRM_LHCL_INPUT_TOPIC']
        validator.validate_all(kafka_config, sr_config, topic)
    
    def _create_kafka_stream(self, kafka_config):
        reader = KafkaStreamReader(
            self.spark, 
            self.config['source_confluent_cloud'], 
            kafka_config
        )
        return reader.create_stream()
    
    def _parse_messages(self, df, sr_config):
        sr_manager = SchemaRegistryManager(sr_config)
        avro_mode = self.config['source_confluent_cloud']['PRM_LHCL_AVRO_OPTION_MODE']
        parser = MessageParser(sr_manager, avro_mode)
        return parser.add_parsed_columns(df)
    
    def _write_to_delta(self, df):
        target_config = self.config['target']
        source_config = self.config['source_confluent_cloud']
        
        table_name = f"{target_config['PRM_LHCL_SCHEMA']}.{target_config['PRM_LHCL_TABLE']}"
        checkpoint = source_config['PRM_LHCL_CHECKPOINT_LOCATION']
        partition_col = source_config.get('PRM_LHCL_PARTITION_COLUMN_NAME')
        
        writer = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint) \
            .option("mergeSchema", "true")
        
        if partition_col:
            writer = writer.partitionBy(partition_col)
        
        # Start the query
        return writer.table(table_name)

def run_pipeline(config_yaml_path: str):
    """
    Entry point for the pipeline.
    """
    pipeline = KafkaIngestionPipeline(config_yaml_path)
    query = pipeline.run()
    # In a Databricks job, we usually want to wait for termination
    # query.awaitTermination() 
    # But for interactive use or testing, we might not.
    # Let's assume this is a job entry point and we should wait?
    # The prompt says "Ejecutar pipeline", usually implies running it.
    # However, blocking here prevents further execution in a notebook.
    # Let's return the query object.
    return query
