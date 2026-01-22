from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafka_ingestion.utils.logger import get_logger
from kafka_ingestion.utils.exceptions import KafkaConnectionError, SchemaRegistryError
import time

logger = get_logger(__name__)

class KafkaConnectionValidator:
    def validate_cluster_connection(self, kafka_config: dict) -> bool:
        """
        Validates connection to the Kafka cluster by listing topics.
        """
        try:
            # AdminClient requires bootstrap.servers and security config
            admin_client = AdminClient(kafka_config)
            
            # List topics with a timeout
            cluster_metadata = admin_client.list_topics(timeout=10)
            
            if not cluster_metadata.brokers:
                logger.error("No brokers found in cluster metadata.")
                return False
                
            logger.info(f"Successfully connected to Kafka cluster. Brokers: {len(cluster_metadata.brokers)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka cluster: {str(e)}")
            return False
    
    def validate_topic_exists(self, topic: str, kafka_config: dict) -> bool:
        """
        Validates that the specified topic exists.
        """
        try:
            admin_client = AdminClient(kafka_config)
            cluster_metadata = admin_client.list_topics(timeout=10)
            
            if topic not in cluster_metadata.topics:
                logger.error(f"Topic '{topic}' not found in cluster.")
                return False
                
            logger.info(f"Topic '{topic}' exists.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate topic existence: {str(e)}")
            return False
    
    def validate_schema_registry_connection(self, sr_config: dict) -> bool:
        """
        Validates connection to Schema Registry by listing subjects.
        """
        try:
            client = SchemaRegistryClient(sr_config)
            # Try to get subjects (lightweight operation)
            client.get_subjects()
            logger.info("Successfully connected to Schema Registry.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Schema Registry: {str(e)}")
            return False
    
    def validate_all(self, kafka_config: dict, sr_config: dict, topic: str):
        """
        Runs all validations and raises KafkaConnectionError if any fail.
        """
        if not self.validate_cluster_connection(kafka_config):
            raise KafkaConnectionError("Could not connect to Kafka cluster.")
            
        if not self.validate_topic_exists(topic, kafka_config):
            raise KafkaConnectionError(f"Topic '{topic}' does not exist.")
            
        if not self.validate_schema_registry_connection(sr_config):
            raise SchemaRegistryError("Could not connect to Schema Registry.")
