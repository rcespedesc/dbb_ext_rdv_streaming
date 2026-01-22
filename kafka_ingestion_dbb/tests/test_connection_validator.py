import pytest
from unittest.mock import patch, MagicMock
from kafka_ingestion.kafka.connection_validator import KafkaConnectionValidator
from kafka_ingestion.utils.exceptions import KafkaConnectionError, SchemaRegistryError

class TestKafkaConnectionValidator:
    @pytest.fixture
    def validator(self):
        return KafkaConnectionValidator()

    @pytest.fixture
    def kafka_config(self):
        return {"bootstrap.servers": "localhost:9092"}

    @pytest.fixture
    def sr_config(self):
        return {"url": "http://localhost:8081"}

    @patch('kafka_ingestion.kafka.connection_validator.AdminClient')
    def test_validate_cluster_connection_success(self, mock_admin_client, validator, kafka_config):
        """Test successful connection validation."""
        mock_client_instance = mock_admin_client.return_value
        mock_metadata = MagicMock()
        mock_metadata.brokers = {1: "broker1"}
        mock_client_instance.list_topics.return_value = mock_metadata

        assert validator.validate_cluster_connection(kafka_config) is True
        mock_admin_client.assert_called_with(kafka_config)

    @patch('kafka_ingestion.kafka.connection_validator.AdminClient')
    def test_validate_cluster_connection_failure_no_brokers(self, mock_admin_client, validator, kafka_config):
        """Test connection validation failure when no brokers found."""
        mock_client_instance = mock_admin_client.return_value
        mock_metadata = MagicMock()
        mock_metadata.brokers = {}
        mock_client_instance.list_topics.return_value = mock_metadata

        assert validator.validate_cluster_connection(kafka_config) is False

    @patch('kafka_ingestion.kafka.connection_validator.AdminClient')
    def test_validate_cluster_connection_exception(self, mock_admin_client, validator, kafka_config):
        """Test connection validation exception."""
        mock_admin_client.side_effect = Exception("Connection refused")
        assert validator.validate_cluster_connection(kafka_config) is False

    @patch('kafka_ingestion.kafka.connection_validator.AdminClient')
    def test_validate_topic_exists_success(self, mock_admin_client, validator, kafka_config):
        """Test topic existence validation success."""
        mock_client_instance = mock_admin_client.return_value
        mock_metadata = MagicMock()
        mock_metadata.topics = {"my-topic": MagicMock()}
        mock_client_instance.list_topics.return_value = mock_metadata

        assert validator.validate_topic_exists("my-topic", kafka_config) is True

    @patch('kafka_ingestion.kafka.connection_validator.AdminClient')
    def test_validate_topic_exists_failure(self, mock_admin_client, validator, kafka_config):
        """Test topic existence validation failure."""
        mock_client_instance = mock_admin_client.return_value
        mock_metadata = MagicMock()
        mock_metadata.topics = {"other-topic": MagicMock()}
        mock_client_instance.list_topics.return_value = mock_metadata

        assert validator.validate_topic_exists("my-topic", kafka_config) is False

    @patch('kafka_ingestion.kafka.connection_validator.SchemaRegistryClient')
    def test_validate_schema_registry_connection_success(self, mock_sr_client, validator, sr_config):
        """Test Schema Registry connection success."""
        mock_client_instance = mock_sr_client.return_value
        mock_client_instance.get_subjects.return_value = ["subject1"]

        assert validator.validate_schema_registry_connection(sr_config) is True

    @patch('kafka_ingestion.kafka.connection_validator.SchemaRegistryClient')
    def test_validate_schema_registry_connection_failure(self, mock_sr_client, validator, sr_config):
        """Test Schema Registry connection failure."""
        mock_sr_client.side_effect = Exception("SR Down")
        assert validator.validate_schema_registry_connection(sr_config) is False

    def test_validate_all_success(self, validator):
        """Test validate_all when everything is fine."""
        validator.validate_cluster_connection = MagicMock(return_value=True)
        validator.validate_topic_exists = MagicMock(return_value=True)
        validator.validate_schema_registry_connection = MagicMock(return_value=True)

        # Should not raise exception
        validator.validate_all({}, {}, "topic")

    def test_validate_all_kafka_failure(self, validator):
        """Test validate_all raises KafkaConnectionError if cluster connection fails."""
        validator.validate_cluster_connection = MagicMock(return_value=False)
        
        with pytest.raises(KafkaConnectionError) as excinfo:
            validator.validate_all({}, {}, "topic")
        assert "Could not connect to Kafka cluster" in str(excinfo.value)
