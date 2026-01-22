import pytest
from unittest.mock import MagicMock
from kafka_ingestion.security.keyvault_manager import KeyVaultManager
from kafka_ingestion.utils.exceptions import KeyVaultError

class TestKeyVaultManager:
    @pytest.fixture
    def mock_dbutils(self):
        mock = MagicMock()
        mock.secrets = MagicMock()
        return mock

    @pytest.fixture
    def kv_manager(self, mock_dbutils):
        return KeyVaultManager(mock_dbutils, "dev")

    def test_init(self, kv_manager):
        assert kv_manager.scope == "kv-dev-scope"

    def test_get_secret_success(self, kv_manager, mock_dbutils):
        """Test successful secret retrieval."""
        mock_dbutils.secrets.get.return_value = "secret-value"
        
        val = kv_manager.get_secret("my-key")
        assert val == "secret-value"
        mock_dbutils.secrets.get.assert_called_with(scope="kv-dev-scope", key="my-key")

    def test_get_secret_cached(self, kv_manager, mock_dbutils):
        """Test that secrets are cached."""
        mock_dbutils.secrets.get.return_value = "secret-value"
        
        # First call
        kv_manager.get_secret("my-key")
        
        # Second call
        kv_manager.get_secret("my-key")
        
        # Should be called only once
        assert mock_dbutils.secrets.get.call_count == 1

    def test_get_secret_not_found_none(self, kv_manager, mock_dbutils):
        """Test raising KeyVaultError when secret is None."""
        mock_dbutils.secrets.get.return_value = None
        
        with pytest.raises(KeyVaultError) as excinfo:
            kv_manager.get_secret("missing-key")
        assert "Secret 'missing-key' not found" in str(excinfo.value)

    def test_get_secret_exception(self, kv_manager, mock_dbutils):
        """Test handling of exceptions from dbutils."""
        mock_dbutils.secrets.get.side_effect = Exception("DBUtils Error")
        
        with pytest.raises(KeyVaultError) as excinfo:
            kv_manager.get_secret("error-key")
        assert "Failed to retrieve secret" in str(excinfo.value)

    def test_get_kafka_config(self, kv_manager):
        """Test constructing kafka config from secrets."""
        kv_manager.get_secret = MagicMock(side_effect=lambda k: f"val-{k}")
        
        config = kv_manager.get_kafka_config()
        assert config["bootstrap.servers"] == "val-kafka-bootstrap-servers"
        assert config["security.protocol"] == "val-kafka-security-protocol"

    def test_get_schema_registry_config(self, kv_manager):
        """Test constructing sr config from secrets."""
        kv_manager.get_secret = MagicMock(side_effect=lambda k: f"val-{k}")
        
        config = kv_manager.get_schema_registry_config()
        assert config["url"] == "val-schema-registry-url"
