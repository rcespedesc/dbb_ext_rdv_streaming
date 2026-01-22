from confluent_kafka.schema_registry import SchemaRegistryClient
from kafka_ingestion.utils.logger import get_logger
from kafka_ingestion.utils.exceptions import SchemaRegistryError
import struct

logger = get_logger(__name__)

class SchemaRegistryManager:
    def __init__(self, schema_registry_config: dict):
        self.client = SchemaRegistryClient(schema_registry_config)
        self.schema_cache = {}
    
    def extract_schema_id(self, message_bytes: bytes) -> int:
        """
        Extracts the Schema ID from the binary message.
        Format: [magic_byte(0x00)][schema_id(4 bytes big-endian)][avro_payload]
        """
        if not message_bytes or len(message_bytes) < 5:
            raise ValueError("Message is too short to contain schema ID")
            
        if message_bytes[0] != 0:
            raise ValueError(f"Incorrect magic byte: {message_bytes[0]}")
            
        # Unpack 4 bytes as big-endian unsigned integer
        schema_id = struct.unpack('>I', message_bytes[1:5])[0]
        return schema_id
    
    def get_schema(self, schema_id: int):
        """
        Retrieves schema from cache or Schema Registry.
        Returns the parsed schema object (Schema).
        """
        if schema_id not in self.schema_cache:
            try:
                logger.info(f"Fetching schema ID {schema_id} from Registry...")
                schema = self.client.get_schema(schema_id)
                self.schema_cache[schema_id] = schema
            except Exception as e:
                raise SchemaRegistryError(f"Failed to fetch schema ID {schema_id}: {str(e)}")
        
        return self.schema_cache[schema_id]
    
    def get_avro_payload(self, message_bytes: bytes) -> bytes:
        """
        Returns the Avro payload without magic byte and schema ID.
        """
        return message_bytes[5:]
