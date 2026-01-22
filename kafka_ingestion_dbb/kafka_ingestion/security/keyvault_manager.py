from kafka_ingestion.utils.exceptions import KeyVaultError
from kafka_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

class KeyVaultManager:
    def __init__(self, dbutils, ambiente: str):
        self.dbutils = dbutils
        self.ambiente = ambiente
        # Scope naming convention: kv-<env>-scope
        self.scope = f"kv-{ambiente}-scope"
        self._secret_cache = {}
        logger.info(f"Initialized KeyVaultManager with scope: {self.scope}")
    
    def get_secret(self, secret_name: str) -> str:
        """
        Retrieves a secret from Azure Key Vault via Databricks secrets.
        Uses caching to avoid repeated calls.
        """
        if secret_name in self._secret_cache:
            return self._secret_cache[secret_name]
        
        try:
            # dbutils.secrets.get raises an error if secret not found? 
            # Documentation says it might return None or raise. We handle generic exception.
            secret_value = self.dbutils.secrets.get(scope=self.scope, key=secret_name)
            
            if secret_value is None:
                raise KeyVaultError(f"Secret '{secret_name}' not found in scope '{self.scope}'")
                
            self._secret_cache[secret_name] = secret_value
            return secret_value
            
        except Exception as e:
            error_msg = f"Failed to retrieve secret '{secret_name}': {str(e)}"
            logger.error(error_msg)
            raise KeyVaultError(error_msg)
    
    def get_kafka_config(self) -> dict:
        """
        Returns the complete Kafka configuration dictionary with secrets.
        """
        try:
            return {
                "bootstrap.servers": self.get_secret("kafka-bootstrap-servers"),
                "security.protocol": self.get_secret("kafka-security-protocol"),
                "sasl.mechanism": self.get_secret("kafka-sasl-mechanism"),
                "sasl.jaas.config": self.get_secret("kafka-sasl-jaas-config"),
            }
        except KeyVaultError:
            # Re-raise to stop execution
            raise

    def get_schema_registry_config(self) -> dict:
        """
        Returns the Schema Registry configuration dictionary with secrets.
        """
        try:
            return {
                "url": self.get_secret("schema-registry-url"),
                "basic.auth.credentials.source": self.get_secret("basic-auth-credentials-source"),
                "basic.auth.user.info": self.get_secret("basic-auth-user-info"),
            }
        except KeyVaultError:
            raise
