import jsonschema
from jsonschema import validate
from kafka_ingestion.config.schema import CONFIG_SCHEMA
from kafka_ingestion.utils.exceptions import ConfigValidationError
from kafka_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

class ConfigValidator:
    def validate(self, config: dict):
        """
        Validates the configuration dictionary against the schema.
        Raises ConfigValidationError if validation fails.
        """
        try:
            logger.info("Validating configuration structure...")
            validate(instance=config, schema=CONFIG_SCHEMA)
            
            # Additional logical validations
            self._validate_logic(config)
            
            logger.info("Configuration validation successful.")
            
        except jsonschema.exceptions.ValidationError as e:
            error_msg = f"Configuration validation failed: {e.message} at path: {'/'.join(str(p) for p in e.path)}"
            logger.error(error_msg)
            raise ConfigValidationError(error_msg)
        except Exception as e:
            logger.error(f"Unexpected error during validation: {str(e)}")
            raise ConfigValidationError(str(e))

    def _validate_logic(self, config: dict):
        """
        Performs logical validations not covered by JSON schema.
        """
        source_config = config['source_confluent_cloud']
        
        # Validate checkpoint location format (basic check)
        checkpoint = source_config['PRM_LHCL_CHECKPOINT_LOCATION']
        if not (checkpoint.startswith('/') or checkpoint.startswith('dbfs:/') or checkpoint.startswith('abfss://')):
            raise ConfigValidationError(f"Invalid checkpoint location format: {checkpoint}")

        # Validate environment
        env = source_config['PRM_LHCL_AMBIENTE']
        valid_envs = ['dev', 'qa', 'prod', 'cert'] # Add other environments if needed
        if env.lower() not in valid_envs:
            logger.warning(f"Environment '{env}' is not standard (dev, qa, prod, cert). Proceeding anyway.")
