import sys
from unittest.mock import MagicMock

# Mock pyspark before any other imports
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.dbutils"] = MagicMock()

# Mock SparkSession
SparkSession = MagicMock()
sys.modules["pyspark.sql"].SparkSession = SparkSession
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()

# Mock confluent_kafka
sys.modules["confluent_kafka"] = MagicMock()
sys.modules["confluent_kafka.admin"] = MagicMock()
sys.modules["confluent_kafka.schema_registry"] = MagicMock()

# Mock fastavro
sys.modules["fastavro"] = MagicMock()

# Mock requests
sys.modules["requests"] = MagicMock()
