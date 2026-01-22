from pyspark.sql import SparkSession, DataFrame
from kafka_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

class KafkaStreamReader:
    def __init__(self, spark: SparkSession, config: dict, kafka_config: dict):
        self.spark = spark
        self.config = config
        self.kafka_config = kafka_config
    
    def create_stream(self) -> DataFrame:
        """
        Creates the streaming DataFrame from Kafka.
        """
        logger.info("Configuring Kafka stream reader...")
        
        # Base Kafka options
        kafka_options = {
            "kafka.bootstrap.servers": self.kafka_config["bootstrap.servers"],
            "subscribe": self.config["PRM_LHCL_INPUT_TOPIC"],
            "startingOffsets": self.config["PRM_LHCL_STARTING_OFFSETS_VALUE"],
            "maxOffsetsPerTrigger": self.config["PRM_LHCL_MAX_OFFSETS_PER_TRIGGER"],
            "kafka.security.protocol": self.kafka_config["security.protocol"],
            "kafka.sasl.mechanism": self.kafka_config["sasl.mechanism"],
            "kafka.sasl.jaas.config": self.kafka_config["sasl.jaas.config"],
            "failOnDataLoss": "false" # Recommended for production to avoid job failure on retention expiry
        }
        
        # Add consumer group if provided
        if "PRM_LHCL_KAFKA_CONSUMER_GROUP" in self.config:
            kafka_options["kafka.group.id"] = self.config["PRM_LHCL_KAFKA_CONSUMER_GROUP"]

        logger.info(f"Reading from topic: {kafka_options['subscribe']}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        
        # Select necessary columns
        # We keep 'value' as binary for parsing later
        # We can also keep headers, key, etc.
        
        include_headers = self.config.get("PRM_LHCL_INCLUDE_HEADERS", False)
        
        cols_to_select = ["value", "key", "topic", "partition", "offset", "timestamp"]
        if include_headers:
            cols_to_select.append("headers")
            
        return df.selectExpr(*cols_to_select)
