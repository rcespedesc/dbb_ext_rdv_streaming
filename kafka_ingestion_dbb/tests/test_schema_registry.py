import pytest
import struct
from unittest.mock import MagicMock, patch
from kafka_ingestion.kafka.schema_registry import SchemaRegistryManager
from kafka_ingestion.utils.exceptions import SchemaRegistryError

class TestSchemaRegistryManager:
    @pytest.fixture
    def sr_config(self):
        return {"url": "http://mock-sr"}

    @pytest.fixture
    def sr_manager(self, sr_config):
        with patch('kafka_ingestion.kafka.schema_registry.SchemaRegistryClient') as mock_client:
            manager = SchemaRegistryManager(sr_config)
            manager.client = mock_client.return_value
            return manager

    def test_extract_schema_id_success(self, sr_manager):
        """Test extracting schema ID from bytes."""
        # Magic byte (0) + Schema ID 123 (big endian) + payload
        schema_id = 123
        msg = b'\x00' + struct.pack('>I', schema_id) + b'payload'
        
        extracted_id = sr_manager.extract_schema_id(msg)
        assert extracted_id == 123

    def test_extract_schema_id_short_message(self, sr_manager):
        """Test error for too short message."""
        msg = b'\x00\x01' # Less than 5 bytes
        with pytest.raises(ValueError) as excinfo:
            sr_manager.extract_schema_id(msg)
        assert "Message is too short" in str(excinfo.value)

    def test_extract_schema_id_bad_magic_byte(self, sr_manager):
        """Test error for incorrect magic byte."""
        msg = b'\x01' + b'\x00\x00\x00\x01' # Magic byte is 1
        with pytest.raises(ValueError) as excinfo:
            sr_manager.extract_schema_id(msg)
        assert "Incorrect magic byte" in str(excinfo.value)

    def test_get_schema_cached(self, sr_manager):
        """Test schema caching."""
        schema_id = 99
        mock_schema = MagicMock()
        sr_manager.client.get_schema.return_value = mock_schema
        
        # First call
        res = sr_manager.get_schema(schema_id)
        assert res == mock_schema
        
        # Second call
        res2 = sr_manager.get_schema(schema_id)
        assert res2 == mock_schema
        
        # Client called only once
        sr_manager.client.get_schema.assert_called_once_with(schema_id)

    def test_get_schema_error(self, sr_manager):
        """Test handling of schema registry errors."""
        sr_manager.client.get_schema.side_effect = Exception("SR Error")
        
        with pytest.raises(SchemaRegistryError) as excinfo:
            sr_manager.get_schema(101)
        assert "Failed to fetch schema ID 101" in str(excinfo.value)

    def test_get_avro_payload(self, sr_manager):
        """Test payload extraction."""
        msg = b'\x00\x00\x00\x00\x01' + b'actual_payload'
        payload = sr_manager.get_avro_payload(msg)
        assert payload == b'actual_payload'
