import pytest
import json
from unittest.mock import MagicMock, patch
from kafka_ingestion.processing.message_parser import MessageParser

class TestMessageParser:
    @pytest.fixture
    def mock_sr_manager(self):
        return MagicMock()

    def test_add_parsed_columns_calls(self, mock_sr_manager):
        """
        Verify that add_parsed_columns constructs the chain of transformations.
        We can't easily execute UDFs with mocked Spark in unit tests without a local spark context,
        so we mostly verify the interactions or test the inner logical function if exposed.
        Here we verify the dataframe method calls.
        """
        parser = MessageParser(mock_sr_manager, "PERMISSIVE")
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df # fluid interface
        
        # Call the method
        parser.add_parsed_columns(mock_df)
        
        # Verify it called withColumn multiple times
        # 1. parsed_data
        # 2. parsed_value
        # 3. schema_id
        # 4. parse_error
        # 5. processing_timestamp
        assert mock_df.withColumn.call_count >= 5

    def test_parse_logic_success(self, mock_sr_manager):
        """
        Manually access the inner processing function of the UDF to test logic independent of Spark.
        """
        parser = MessageParser(mock_sr_manager, "PERMISSIVE")
        
        # Mock SR manager
        mock_sr_manager.extract_schema_id.return_value = 1
        mock_schema = MagicMock()
        mock_schema.schema_str = "{}"
        mock_sr_manager.get_schema.return_value = mock_schema
        mock_sr_manager.get_avro_payload.return_value = b'payload'
        
        # Mock fastavro in the context of the module
        with patch('kafka_ingestion.processing.message_parser.fastavro') as mock_fastavro:
            mock_fastavro.reader.return_value = iter([{"col": "val"}])
            
            # Extract the inner function from the UDF
            # The public method returns a UDF object. 
            # In our mock environment, udf() returns a MagicMock, so we can't get the python function back easily
            # unless we didn't mock udf. 
            # But we mocked pyspark.sql.functions.udf in conftest.py.
            
            # STRATEGY: We will re-implement the inner logic extraction or refactor the class to make it testable.
            # Best way: Refactor class to expose `_parse_message` as a static method or instance method, 
            # but I can't change the code right now without approval or good reason.
            # Alternative: Since we are in python, we can inspect the source or just duplicate the logic test.
            # actually, if we inspect the `parse_avro_message_udf` method, we can see it defines `parse_message` locally.
            # We can't access it.
            
            # Let's rely on patching `udf` in this test specifically to capture the function.
            
            captured_func = []
            def capture_udf(f, returnType=None):
                captured_func.append(f)
                return MagicMock()

            with patch('kafka_ingestion.processing.message_parser.udf', side_effect=capture_udf):
                parser.parse_avro_message_udf()
            
            inner_func = captured_func[0]
            
            # Run the inner function
            result_json, result_id, result_error = inner_func(b'some_bytes')
            
            assert result_id == 1
            assert result_error is None
            assert json.loads(result_json) == {"col": "val"}

    def test_parse_logic_failfast(self, mock_sr_manager):
        """Test FAILFAST mode raises exception."""
        parser = MessageParser(mock_sr_manager, "FAILFAST")
        mock_sr_manager.extract_schema_id.side_effect = Exception("Parsing Error")
        
        captured_func = []
        def capture_udf(f, returnType=None):
            captured_func.append(f)
            return MagicMock()

        with patch('kafka_ingestion.processing.message_parser.udf', side_effect=capture_udf):
            parser.parse_avro_message_udf()
        
        inner_func = captured_func[0]
        
        with pytest.raises(Exception) as excinfo:
            inner_func(b'bad_bytes')
        assert "Parsing failed in FAILFAST mode" in str(excinfo.value)

    def test_parse_logic_permissive(self, mock_sr_manager):
        """Test PERMISSIVE mode returns error."""
        parser = MessageParser(mock_sr_manager, "PERMISSIVE")
        mock_sr_manager.extract_schema_id.side_effect = Exception("Parsing Error")
        
        captured_func = []
        def capture_udf(f, returnType=None):
            captured_func.append(f)
            return MagicMock()

        with patch('kafka_ingestion.processing.message_parser.udf', side_effect=capture_udf):
            parser.parse_avro_message_udf()
        
        inner_func = captured_func[0]
        
        result_json, result_id, result_error = inner_func(b'bad_bytes')
        assert result_json is None
        assert result_id is None
        assert "Parsing Error" in str(result_error)
