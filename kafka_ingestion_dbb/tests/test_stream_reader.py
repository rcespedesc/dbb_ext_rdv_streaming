import pytest
from unittest.mock import MagicMock
from kafka_ingestion.kafka.stream_reader import KafkaStreamReader

class TestKafkaStreamReader:
    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

    @pytest.fixture
    def config(self):
        return {
            "PRM_LHCL_INPUT_TOPIC": "test-topic",
            "PRM_LHCL_STARTING_OFFSETS_VALUE": "earliest",
            "PRM_LHCL_MAX_OFFSETS_PER_TRIGGER": 100,
            "PRM_LHCL_INCLUDE_HEADERS": False
        }

    @pytest.fixture
    def kafka_config(self):
        return {
            "bootstrap.servers": "broker:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": "..."
        }

    def test_create_stream_options(self, mock_spark, config, kafka_config):
        """Test that readStream is configured with correct options."""
        reader = KafkaStreamReader(mock_spark, config, kafka_config)
        
        # Setup mock chain
        mock_read_stream = mock_spark.readStream
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.options.return_value = mock_read_stream
        mock_read_stream.load.return_value = MagicMock() # DF
        
        # Call
        reader.create_stream()
        
        # Verify format
        mock_read_stream.format.assert_called_with("kafka")
        
        # Verify options
        # We check that options was called. Get the args.
        call_args = mock_read_stream.options.call_args[1] # kwargs
        assert call_args["kafka.bootstrap.servers"] == "broker:9092"
        assert call_args["subscribe"] == "test-topic"
        assert call_args["startingOffsets"] == "earliest"

    def test_create_stream_with_headers(self, mock_spark, config, kafka_config):
        """Test that headers are selected if configured."""
        config["PRM_LHCL_INCLUDE_HEADERS"] = True
        reader = KafkaStreamReader(mock_spark, config, kafka_config)
        
        mock_df = MagicMock()
        mock_spark.readStream.load.return_value = mock_df
        mock_spark.readStream.format.return_value = mock_spark.readStream
        mock_spark.readStream.options.return_value = mock_spark.readStream
        
        reader.create_stream()
        
        # Verify selectExpr includes headers
        # args[0] in selectExpr(*args) matches the unpacked list
        select_call = mock_df.selectExpr.call_args
        selected_cols = select_call[0]
        assert "headers" in selected_cols
