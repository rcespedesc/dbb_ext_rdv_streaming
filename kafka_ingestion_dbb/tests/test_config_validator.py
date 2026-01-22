import pytest
from kafka_ingestion.config.validator import ConfigValidator
from kafka_ingestion.utils.exceptions import ConfigValidationError

class TestConfigValidator:
    @pytest.fixture
    def validator(self):
        return ConfigValidator()

    @pytest.fixture
    def valid_config(self):
        return {
            "source": {
                "SBB": "ingesta-eventos",
                "SBB_v": "1.0.0",
                "type": "kafka"
            },
            "source_confluent_cloud": {
                "PRM_LHCL_AMBIENTE": "dev",
                "PRM_LHCL_INCLUDE_HEADERS": False,
                "PRM_LHCL_USE_SCHEMA_REGISTRY": True,
                "PRM_LHCL_CHECKPOINT_LOCATION": "/mnt/datalake/checkpoints/mi_tabla",
                "PRM_LHCL_APPLICATION": "mi-app",
                "PRM_LHCL_INPUT_TOPIC": "mi.topico.v1",
                "PRM_LHCL_KAFKA_CONSUMER_GROUP": "mi-consumer-group",
                "PRM_LHCL_MAX_OFFSETS_PER_TRIGGER": 10000,
                "PRM_LHCL_STARTING_OFFSETS_VALUE": "earliest",
                "PRM_LHCL_AVRO_OPTION_MODE": "PERMISSIVE",
                "PRM_LHCL_PARTITION_COLUMN_NAME": "processing_date"
            },
            "target": {
                "PRM_LHCL_TABLE": "mi_tabla",
                "PRM_LHCL_SCHEMA": "mi_schema",
                "PRM_LHCL_TABLE_DESCRIPTION": "Tabla de eventos"
            }
        }

    def test_validate_valid_config(self, validator, valid_config):
        """Test that a valid configuration passes validation."""
        try:
            validator.validate(valid_config)
        except ConfigValidationError:
            pytest.fail("ConfigValidationError raised for valid config")

    def test_validate_invalid_schema_missing_field(self, validator, valid_config):
        """Test that missing required field raises validation error."""
        del valid_config['source']['type']
        with pytest.raises(ConfigValidationError) as excinfo:
            validator.validate(valid_config)
        assert "Configuration validation failed" in str(excinfo.value)

    def test_validate_invalid_schema_wrong_type(self, validator, valid_config):
        """Test that wrong field type raises validation error."""
        valid_config['source_confluent_cloud']['PRM_LHCL_MAX_OFFSETS_PER_TRIGGER'] = "not-an-integer"
        with pytest.raises(ConfigValidationError) as excinfo:
            validator.validate(valid_config)
        assert "Configuration validation failed" in str(excinfo.value)

    def test_validate_logic_invalid_checkpoint(self, validator, valid_config):
        """Test invalid checkpoint location format."""
        valid_config['source_confluent_cloud']['PRM_LHCL_CHECKPOINT_LOCATION'] = "invalid/path"
        with pytest.raises(ConfigValidationError) as excinfo:
            validator.validate(valid_config)
        assert "Invalid checkpoint location format" in str(excinfo.value)

    def test_validate_logic_non_standard_env(self, validator, valid_config, caplog):
        """Test warning for non-standard environment."""
        valid_config['source_confluent_cloud']['PRM_LHCL_AMBIENTE'] = "staging"
        validator.validate(valid_config)
        assert "Environment 'staging' is not standard" in caplog.text
