from pyspark.sql.functions import udf, col, current_timestamp, struct
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import fastavro
from io import BytesIO
import json
from kafka_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

class MessageParser:
    def __init__(self, schema_registry_manager, avro_mode: str):
        self.sr_manager = schema_registry_manager
        self.avro_mode = avro_mode
    
    def parse_avro_message_udf(self):
        """
        Returns a Spark UDF to parse Avro messages.
        Output schema: StructType(json, schema_id, error)
        """
        # Define return schema for UDF
        return_schema = StructType([
            StructField("json", StringType(), True),
            StructField("schema_id", IntegerType(), True),
            StructField("error", StringType(), True)
        ])

        def parse_message(message_bytes):
            if message_bytes is None:
                return (None, None, "Message is None")
                
            try:
                # 1. Extract Schema ID
                schema_id = self.sr_manager.extract_schema_id(message_bytes)
                
                # 2. Get Schema
                schema = self.sr_manager.get_schema(schema_id)
                
                # 3. Get Payload
                payload = self.sr_manager.get_avro_payload(message_bytes)
                
                # 4. Deserialize
                bytes_reader = BytesIO(payload)
                # schema.schema_str gives the JSON string representation of the schema
                reader = fastavro.reader(bytes_reader, schema.schema_str)
                record = next(reader)
                
                # 5. Return as JSON
                return (json.dumps(record), schema_id, None)
                
            except Exception as e:
                error_msg = str(e)
                if self.avro_mode == "FAILFAST":
                    # In FAILFAST, we raise the exception to stop the stream
                    raise Exception(f"Parsing failed in FAILFAST mode: {error_msg}")
                
                # In PERMISSIVE, we return the error
                return (None, None, error_msg)
        
        return udf(parse_message, return_schema)
    
    def add_parsed_columns(self, df):
        """
        Adds parsed columns to the DataFrame.
        """
        logger.info(f"Adding parsed columns with mode: {self.avro_mode}")
        parse_udf = self.parse_avro_message_udf()
        
        # Apply UDF
        df_parsed = df.withColumn(
            "parsed_data", 
            parse_udf(col("value"))
        )
        
        # Flatten structure
        df_final = df_parsed \
            .withColumn("parsed_value", col("parsed_data.json")) \
            .withColumn("schema_id", col("parsed_data.schema_id")) \
            .withColumn("parse_error", col("parsed_data.error")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .drop("parsed_data")
        
        return df_final
