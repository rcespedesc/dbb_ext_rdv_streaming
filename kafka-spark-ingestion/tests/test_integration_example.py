# import unittest
# from kafka_spark_ingestion.pipeline import KafkaSparkPipeline
# from pyspark.sql import SparkSession

# class TestIntegration(unittest.TestCase):
#     """
#     Integration tests requiring real Kafka and Schema Registry.
#     Uncomment and configure to run against actual infrastructure.
#     """
#     
#     def setUp(self):
#         # Ensure config points to real dev environment
#         self.config_path = "config/dev_integration_config.yml"
#         self.pipeline = KafkaSparkPipeline(self.config_path)
#
#     def test_end_to_end_flow(self):
#         # 1. Validate connections
#         self.assertTrue(self.pipeline.validate())
#
#         # 2. Run pipeline (assuming batch mode for test or short streaming trigger)
#         # Note: For streaming, you might want to run in a separate thread or use AvailableNow trigger
#         try:
#             self.pipeline.run()
#         except Exception as e:
#             self.fail(f"Pipeline execution failed: {e}")
#
#     def test_schema_registry_integration(self):
#         # Test specifically if we can fetch schema
#         schema = self.pipeline.schema_handler.get_schema(self.pipeline.config['kafka']['topic'])
#         self.assertIsNotNone(schema)
