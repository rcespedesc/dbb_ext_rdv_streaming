from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Dict, Any

class KafkaReader:
    """
    Handles Kafka connection validation and Spark DataFrame creation.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the KafkaReader.

        Args:
            spark (SparkSession): The active Spark session.
            config (Dict[str, Any]): Kafka configuration section.
            logger (logging.Logger): Logger instance.
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def validate_topic(self) -> bool:
        """
        Validates that the configured topic exists in Kafka.

        Returns:
            bool: True if topic exists, False otherwise.
        """
        conf = {'bootstrap.servers': self.config['bootstrap_servers']}
        # Add security config if needed (SASL/SSL) - simplified for now
        if 'security.protocol' in self.config:
            conf['security.protocol'] = self.config['security.protocol']
            conf['sasl.mechanism'] = self.config.get('sasl.mechanism', 'PLAIN')
            conf['sasl.username'] = self.config.get('sasl.username')
            conf['sasl.password'] = self.config.get('sasl.password')

        try:
            admin_client = AdminClient(conf)
            topics = admin_client.list_topics(timeout=10).topics
            if self.config['topic'] in topics:
                self.logger.info(f"Topic '{self.config['topic']}' exists.")
                return True
            else:
                self.logger.error(f"Topic '{self.config['topic']}' does not exist. Available topics: {list(topics.keys())[:10]}...")
                return False
        except Exception as e:
            self.logger.error(f"Failed to validate Kafka topic: {e}")
            return False

    def read_stream(self) -> DataFrame:
        """
        Reads data from Kafka as a streaming DataFrame.

        Returns:
            DataFrame: The raw Kafka DataFrame.
        """
        self.logger.info(f"Starting read stream from topic: {self.config['topic']}")
        
        options = {
            "kafka.bootstrap.servers": self.config['bootstrap_servers'],
            "subscribe": self.config['topic'],
            "startingOffsets": self.config.get("starting_offsets", "latest"),
            "failOnDataLoss": self.config.get("fail_on_data_loss", "false")
        }
        
        # Add optional security options
        if 'security.protocol' in self.config:
            options["kafka.security.protocol"] = self.config['security.protocol']
            options["kafka.sasl.mechanism"] = self.config.get('sasl.mechanism', 'PLAIN')
            # Note: For Spark, JAAS config is usually passed via spark-submit or extraJavaOptions
            # But we can try to pass it if supported by the connector version or environment
            
        return self.spark.readStream \
            .format("kafka") \
            .options(**options) \
            .load()

    def read_batch(self) -> DataFrame:
        """
        Reads data from Kafka as a batch DataFrame.

        Returns:
            DataFrame: The raw Kafka DataFrame.
        """
        self.logger.info(f"Starting batch read from topic: {self.config['topic']}")
        
        options = {
            "kafka.bootstrap.servers": self.config['bootstrap_servers'],
            "subscribe": self.config['topic'],
            "startingOffsets": self.config.get("starting_offsets", "earliest"),
            "endingOffsets": self.config.get("ending_offsets", "latest"),
            "failOnDataLoss": self.config.get("fail_on_data_loss", "false")
        }

        if 'security.protocol' in self.config:
            options["kafka.security.protocol"] = self.config['security.protocol']
        
        return self.spark.read \
            .format("kafka") \
            .options(**options) \
            .load()
