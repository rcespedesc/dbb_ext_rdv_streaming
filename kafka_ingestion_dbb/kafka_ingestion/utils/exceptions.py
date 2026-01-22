class KafkaIngestionException(Exception):
    """Base exception for the package"""
    pass

class ConfigValidationError(KafkaIngestionException):
    """Error in configuration validation"""
    pass

class KeyVaultError(KafkaIngestionException):
    """Error retrieving secrets"""
    pass

class KafkaConnectionError(KafkaIngestionException):
    """Error connecting to Kafka"""
    pass

class SchemaRegistryError(KafkaIngestionException):
    """Error interacting with Schema Registry"""
    pass

class MessageParsingError(KafkaIngestionException):
    """Error parsing messages"""
    pass
