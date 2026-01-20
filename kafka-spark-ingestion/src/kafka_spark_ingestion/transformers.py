from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, struct, to_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, TimestampType, BinaryType, IntegerType
import avro.schema
import avro.io
import io
import json
import logging
from typing import Optional

class DataTransformer:
    """
    Handles deserialization and transformation of Kafka data.
    """

    def __init__(self, schema_str: str, schema_type: str = "AVRO", logger: logging.Logger = None):
        """
        Initialize the DataTransformer.

        Args:
            schema_str (str): The schema string (JSON format for Avro/JSON Schema).
            schema_type (str): Type of schema ('AVRO', 'JSON', 'PROTOBUF').
            logger (logging.Logger): Logger instance.
        """
        self.schema_str = schema_str
        self.schema_type = schema_type.upper()
        self.logger = logger or logging.getLogger(__name__)

    def _get_avro_deserializer(self):
        """
        Returns a function to deserialize Avro data.
        """
        # Parse schema once
        try:
            schema = avro.schema.parse(self.schema_str)
        except Exception as e:
            self.logger.error(f"Failed to parse Avro schema: {e}")
            raise

        def deserialize(payload):
            if payload is None:
                return None
            try:
                # Skip Confluent Magic Byte (0) and Schema ID (4 bytes)
                # Total 5 bytes header
                if len(payload) < 5:
                    return None
                
                # We assume the schema is correct for the ID in the payload
                # In a robust prod system, we might need to look up schema by ID dynamically
                # but here we use the topic-value schema strategy.
                
                bytes_reader = io.BytesIO(payload[5:])
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = avro.io.DatumReader(schema)
                return json.dumps(reader.read(decoder))
            except Exception as e:
                # Log error in UDF is tricky, usually we return null or error struct
                return json.dumps({"error": str(e), "raw_hex": payload.hex() if payload else None})
        
        return deserialize

    def _get_json_deserializer(self):
        """
        Returns a function to deserialize JSON data.
        """
        def deserialize(payload):
            if payload is None:
                return None
            try:
                # Skip header if present (Confluent JSON also has 5 byte header)
                # But sometimes raw JSON is sent. We'll try to detect or just skip if we know it's Confluent.
                # Assuming Confluent JSON with schema registry
                start_idx = 5 if len(payload) > 5 and payload[0] == 0 else 0
                data_str = payload[start_idx:].decode('utf-8')
                return data_str # It's already JSON
            except Exception as e:
                return json.dumps({"error": str(e)})
        return deserialize

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applies deserialization to the DataFrame.

        Args:
            df (DataFrame): Input DataFrame with 'value' (binary) and metadata columns.

        Returns:
            DataFrame: Transformed DataFrame with 'message_json' and metadata.
        """
        if self.schema_type == "AVRO":
            deserializer_func = self._get_avro_deserializer()
        elif self.schema_type == "JSON":
            deserializer_func = self._get_json_deserializer()
        else:
            self.logger.warning(f"Unsupported schema type: {self.schema_type}. Returning raw value as string.")
            deserializer_func = lambda x: str(x) if x else None

        deserialize_udf = udf(deserializer_func, StringType())

        # Select and rename columns to match output requirement
        # Input df from Kafka has: key, value, topic, partition, offset, timestamp, timestampType
        
        return df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("key").cast("string").alias("key"),
            deserialize_udf(col("value")).alias("message_json"),
            col("value").alias("raw_value")
        )
