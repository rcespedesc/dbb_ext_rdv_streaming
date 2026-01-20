import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import yaml

# Mock pyspark and confluent_kafka before importing package modules
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["confluent_kafka"] = MagicMock()
sys.modules["confluent_kafka.schema_registry"] = MagicMock()
sys.modules["confluent_kafka.schema_registry.avro"] = MagicMock()
sys.modules["confluent_kafka.schema_registry.json_schema"] = MagicMock()
sys.modules["confluent_kafka.schema_registry.protobuf"] = MagicMock()
sys.modules["confluent_kafka.serialization"] = MagicMock()
sys.modules["confluent_kafka.admin"] = MagicMock()
sys.modules["avro"] = MagicMock()
sys.modules["avro.schema"] = MagicMock()
sys.modules["avro.io"] = MagicMock()

# Now import the package modules
from kafka_spark_ingestion.config_loader import ConfigLoader
from kafka_spark_ingestion.pipeline import KafkaSparkPipeline

class TestConfigLoader(unittest.TestCase):
    def setUp(self):
        self.test_config_path = "test_config.yml"
        self.config_data = {
            "kafka": {"bootstrap_servers": "localhost:9092", "topic": "test", "consumer_group": "group"},
            "schema_registry": {"url": "http://localhost:8081"},
            "spark": {"app_name": "TestApp"},
            "output": {"format": "console", "mode": "append", "checkpoint_location": "/tmp"}
        }
        with open(self.test_config_path, "w") as f:
            yaml.dump(self.config_data, f)

    def tearDown(self):
        if os.path.exists(self.test_config_path):
            os.remove(self.test_config_path)

    def test_load_config(self):
        loader = ConfigLoader(self.test_config_path)
        self.assertEqual(loader.config["kafka"]["topic"], "test")

    def test_validate_config(self):
        loader = ConfigLoader(self.test_config_path)
        self.assertTrue(loader.validate())

    def test_missing_config(self):
        bad_config = {"kafka": {}}
        with open("bad_config.yml", "w") as f:
            yaml.dump(bad_config, f)
        
        loader = ConfigLoader("bad_config.yml")
        with self.assertRaises(ValueError):
            loader.validate()
        os.remove("bad_config.yml")

class TestPipeline(unittest.TestCase):
    @patch("kafka_spark_ingestion.pipeline.SchemaHandler")
    @patch("kafka_spark_ingestion.pipeline.KafkaReader")
    @patch("kafka_spark_ingestion.pipeline.SparkSession")
    def test_validation_flow(self, mock_spark, mock_kafka_reader, mock_schema_handler):
        # Setup mocks
        mock_schema_instance = mock_schema_handler.return_value
        mock_schema_instance.validate_registry_connection.return_value = True
        mock_schema_instance.get_schema.return_value = '{"type": "string"}'
        
        mock_kafka_instance = mock_kafka_reader.return_value
        mock_kafka_instance.validate_topic.return_value = True

        # Create dummy config
        config_data = {
            "kafka": {"bootstrap_servers": "localhost:9092", "topic": "test", "consumer_group": "group"},
            "schema_registry": {"url": "http://localhost:8081"},
            "spark": {"app_name": "TestApp"},
            "output": {"format": "console", "mode": "append", "checkpoint_location": "/tmp"}
        }
        with open("test_pipeline_config.yml", "w") as f:
            yaml.dump(config_data, f)

        pipeline = KafkaSparkPipeline("test_pipeline_config.yml")
        self.assertTrue(pipeline.validate())
        
        os.remove("test_pipeline_config.yml")

if __name__ == "__main__":
    unittest.main()
