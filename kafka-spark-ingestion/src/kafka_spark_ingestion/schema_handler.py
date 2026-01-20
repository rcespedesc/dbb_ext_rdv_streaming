from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import logging
from typing import Dict, Any, Optional

class SchemaHandler:
    """
    Handles interactions with Confluent Schema Registry.
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the SchemaHandler.

        Args:
            config (Dict[str, Any]): Schema Registry configuration.
            logger (logging.Logger): Logger instance.
        """
        self.config = config
        self.logger = logger
        self.client = self._create_client()
        self.schema_cache = {}

    def _create_client(self) -> SchemaRegistryClient:
        """
        Creates a Schema Registry client.

        Returns:
            SchemaRegistryClient: The client instance.
        """
        conf = {'url': self.config['url']}
        # Add authentication if provided in config (basic auth example)
        if 'basic.auth.user.info' in self.config:
            conf['basic.auth.user.info'] = self.config['basic.auth.user.info']
            conf['basic.auth.credentials.source'] = 'USER_INFO'
        
        try:
            return SchemaRegistryClient(conf)
        except Exception as e:
            self.logger.error(f"Failed to create Schema Registry client: {e}")
            raise

    def get_schema(self, topic: str, is_key: bool = False) -> Optional[str]:
        """
        Retrieves the schema for a given topic.
        Assumes Subject Name Strategy: {topic}-value or {topic}-key

        Args:
            topic (str): The Kafka topic name.
            is_key (bool): True if retrieving key schema, False for value.

        Returns:
            Optional[str]: The schema string if found, None otherwise.
        """
        suffix = "key" if is_key else "value"
        subject = f"{topic}-{suffix}"

        if subject in self.schema_cache:
            return self.schema_cache[subject]

        try:
            schema = self.client.get_latest_version(subject)
            self.schema_cache[subject] = schema.schema.schema_str
            self.logger.info(f"Retrieved schema for subject: {subject}")
            return schema.schema.schema_str
        except Exception as e:
            self.logger.error(f"Failed to retrieve schema for subject {subject}: {e}")
            return None

    def validate_registry_connection(self) -> bool:
        """
        Checks connectivity to the Schema Registry.

        Returns:
            bool: True if connected, False otherwise.
        """
        try:
            # Attempt to list subjects as a connectivity check
            self.client.get_subjects()
            self.logger.info("Successfully connected to Schema Registry")
            return True
        except Exception as e:
            self.logger.error(f"Could not connect to Schema Registry: {e}")
            return False
