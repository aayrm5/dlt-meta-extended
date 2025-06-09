"""
AB Cancel Translator integration module for DLT-Meta Framework
"""
import logging
import json
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.types import StringType, StructType, StructField, MapType

# Import AB Cancel Translator components
try:
    from ab_cancel_translator_stubs import (
        create_translator, Msg, TranslationResult, CancelType, ErrorCode
    )
    from ab_cancel_translator_stubs.integration import (
        IntegrationHelper, ValidationHelper, MessageBuilder
    )
    AB_TRANSLATOR_AVAILABLE = True
except ImportError:
    AB_TRANSLATOR_AVAILABLE = False

logger = logging.getLogger('databricks.labs.dltmeta')

class ABCancelTranslatorPipeline:
    """AB Cancel Translator integration for DLT-Meta pipelines"""
    
    def __init__(self, spark, dataflow_spec):
        self.spark = spark
        self.dataflow_spec = dataflow_spec
        self.translator = None
        self.integration_helper = None
        
        if AB_TRANSLATOR_AVAILABLE:
            self.translator = create_translator()
            self.integration_helper = IntegrationHelper(self.translator)
        else:
            logger.warning("AB Cancel Translator Stubs not available")
    
    def is_ab_message_format(self) -> bool:
        """Check if this dataflow uses AB message format"""
        if not hasattr(self.dataflow_spec, 'sourceFormat'):
            return False
        return self.dataflow_spec.sourceFormat.lower() in ['ab_binary_messages', 'ab_cancel_messages']
    
    def translate_ab_messages(self, input_df: DataFrame) -> DataFrame:
        """Translate AB cancel messages in the dataframe"""
        if not AB_TRANSLATOR_AVAILABLE or not self.translator:
            logger.error("AB Cancel Translator not available")
            return input_df
        
        # Register UDF for AB message translation
        translate_udf = udf(self._translate_message, StringType())
        
        # Apply translation to binary data column
        binary_col = self._get_binary_column_name(input_df)
        if binary_col:
            translated_df = input_df.withColumn(
                "ab_translated_data", 
                translate_udf(col(binary_col))
            )
            
            # Add parsed fields
            translated_df = self._add_parsed_fields(translated_df)
            return translated_df
        
        return input_df
    
    def _translate_message(self, binary_data: bytes) -> str:
        """UDF function to translate AB binary message"""
        try:
            if not binary_data:
                return json.dumps({"success": False, "error": "Empty binary data"})
            
            # Create Msg object
            msg = Msg(buffer=binary_data, error_code=0, system_name="AB")
            
            # Translate message
            result = self.translator.translate_message(msg)
            
            # Return result as JSON
            return json.dumps({
                "success": result.success,
                "data": result.data,
                "error_message": result.error_message,
                "field_count": result.field_count,
                "cancel_type": result.cancel_type
            })
        except Exception as e:
            return json.dumps({"success": False, "error": str(e)})
    
    def _get_binary_column_name(self, df: DataFrame) -> Optional[str]:
        """Find the binary data column in the dataframe"""
        # Common binary column names
        binary_columns = ['binary_data', 'raw_data', 'message_data', 'value', 'content']
        
        for col_name in df.columns:
            if col_name.lower() in binary_columns:
                return col_name
        
        # If no standard column found, use the first binary-type column
        for field in df.schema.fields:
            if field.dataType.typeName() == 'binary':
                return field.name
        
        return None
    
    def _add_parsed_fields(self, df: DataFrame) -> DataFrame:
        """Add parsed fields from translated AB messages"""
        # Parse JSON result and extract fields
        df = df.withColumn("ab_translation_result", 
                          expr("from_json(ab_translated_data, 'success boolean, data string, error_message string, field_count int, cancel_type int')"))
        
        # Add individual fields
        df = df.withColumn("ab_success", col("ab_translation_result.success"))
        df = df.withColumn("ab_data", col("ab_translation_result.data"))
        df = df.withColumn("ab_error_message", col("ab_translation_result.error_message"))
        df = df.withColumn("ab_field_count", col("ab_translation_result.field_count"))
        df = df.withColumn("ab_cancel_type", col("ab_translation_result.cancel_type"))
        
        # Parse delimited fields if translation was successful
        df = df.withColumn("ab_parsed_fields", 
                          expr("case when ab_success then split(ab_data, '~\\\\|~') else array() end"))
        
        return df
    
    def validate_ab_messages(self, df: DataFrame) -> DataFrame:
        """Validate AB messages using AB Cancel Translator"""
        if not AB_TRANSLATOR_AVAILABLE:
            return df
        
        validate_udf = udf(self._validate_message, StringType())
        binary_col = self._get_binary_column_name(df)
        
        if binary_col:
            df = df.withColumn("ab_validation_result", validate_udf(col(binary_col)))
            df = df.withColumn("ab_is_valid", 
                              expr("from_json(ab_validation_result, 'is_valid boolean, error string').is_valid"))
        
        return df
    
    def _validate_message(self, binary_data: bytes) -> str:
        """UDF function to validate AB binary message"""
        try:
            if not binary_data:
                return json.dumps({"is_valid": False, "error": "Empty binary data"})
            
            msg = Msg(buffer=binary_data, error_code=0, system_name="AB")
            is_valid = self.translator.validate_message(msg)
            
            return json.dumps({"is_valid": is_valid, "error": None})
        except Exception as e:
            return json.dumps({"is_valid": False, "error": str(e)})
