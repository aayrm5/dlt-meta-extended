"""Dataflow utility functions for shared functionality."""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, explode_outer, col, current_timestamp, lit, create_map
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType, DecimalType, IntegerType, TimestampType, BooleanType, BinaryType
from decimal import Decimal
import pyspark.sql.types as T
import json

logger = logging.getLogger('databricks.labs.dltmeta')

class DataflowUtils:
    """Utility class for common dataflow operations."""
    
    @staticmethod
    def recurFlattenDF(dfNested: DataFrame, arrayFieldToExtract: str = "", level: int = 0) -> DataFrame:
        """
        Recursively flatten nested DataFrame structures.
        
        Args:
            dfNested: DataFrame to flatten
            arrayFieldToExtract: Specific array field to extract
            level: Current recursion level
            
        Returns:
            DataFrame: Flattened DataFrame
        """
        # Maximum depth check
        if level > 100:  # Prevent infinite recursion
            logger.warning(f"Maximum recursion depth reached at level {level}")
            logger.warning(f"Schema: {dfNested.schema.simpleString()}")
            return dfNested
        
        # Check if DataFrame is empty
        if not dfNested.columns:
            return dfNested

        rootArrayTypeCounts = 0
        arrayFieldNames = []

        # Identify Array fields in the schema
        logger.info(f"Processing level {level}")
        
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, ArrayType):
                arrayFieldNames.append(field.name)
                if level == 0:
                    rootArrayTypeCounts = rootArrayTypeCounts + 1
                
                # Handle nested arrays (level > 0) when no specific field is targeted
                if level > 0 and arrayFieldToExtract == "":
                    dfNested = dfNested.withColumn(f"{field.name}", explode_outer(f"{field.name}"))

        # Multiple Array Columns Scenario
        if ((rootArrayTypeCounts > 1) or (rootArrayTypeCounts == 1 and len(dfNested.columns) > 1)) and arrayFieldToExtract == "":
            raise ValueError(f"Detected multiple columns which cannot be flattened without providing column name to be extracted, array column names : {str(arrayFieldNames)} and dataset schema : {str(dfNested.columns)}")
        
        # Single Array Column Scenario
        elif rootArrayTypeCounts == 1 and len(dfNested.columns) == 1:
            dfNested = dfNested.withColumn(f"{arrayFieldNames[0]}", explode(f"{arrayFieldNames[0]}")).select(f"{arrayFieldNames[0]}.*")
        
        # Specific Array Column Extraction
        elif (arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns and 
              isinstance(dfNested.select(arrayFieldToExtract).schema.fields[0].dataType, ArrayType)):
            dfNested = dfNested.withColumn(arrayFieldToExtract, explode(arrayFieldToExtract))
            dfNestedlist = dfNested.columns
            dfNestedlist.remove(arrayFieldToExtract)
            dfNestedlist.append(f"{arrayFieldToExtract}.*")
            dfNested = dfNested.select(dfNestedlist)
            logger.info("Processed array data type case")
            arrayFieldToExtract = ""  # Reset for next iteration
        
        # Specific Struct Column Extraction
        elif (arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns and 
              isinstance(dfNested.select(arrayFieldToExtract).schema.fields[0].dataType, StructType)):
            dfNestedlist = dfNested.columns
            dfNestedlist.remove(arrayFieldToExtract)
            dfNestedlist.append(f"{arrayFieldToExtract}.*")
            dfNested = dfNested.select(dfNestedlist)
            arrayFieldToExtract = ""  # Reset for next iteration
        
        # Handle other specified fields
        elif arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns:
            dfNestedlist = dfNested.columns           
            dfNested = dfNested.select(dfNestedlist)                     
            arrayFieldToExtract = ""  # Reset for next iteration
        
        # Flatten all StructType Fields
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, StructType):
                for nested_col in dfNested.select(f"{field.name}.*").columns:
                    logger.info(f"Flattening struct field: {field.name}.{nested_col}")
                    # Create new column with flattened name
                    dfNested = dfNested.withColumn(f"{field.name}__{nested_col}", col(f"{field.name}.{nested_col}"))
                # Drop the original struct column
                dfNested = dfNested.drop(field.name)

        # Check for remaining nested columns
        nested_cols = []
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                nested_cols.append(field)
        
        # Recursive call or return
        if len(nested_cols) == 0:
            return dfNested  # Base case: no more nested structures
        elif len(nested_cols) > 0:
            return DataflowUtils.recurFlattenDF(dfNested, arrayFieldToExtract, level + 1)
        else:
            return dfNested
        


    def get_bronze_schema(self, metadata_file):
        """Get schema from metadafile in json format.

        Args:
            metadata_file ([string]): metadata schema file path
        """
        ddlSchemaStr = self.spark.read.text(
            paths=metadata_file, wholetext=True
        ).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        logger.info(spark_schema)
        schema = json.dumps(spark_schema.jsonValue())
        return schema
    

    def add_extra_record(self, streaming_df, transaction_type):
        """Adds a extra records to the BETFO & PMU Gold Streaming tables using rate stream."""
        
        try:
            print(f"=== Adding dummy record for {transaction_type} ===")
            
            # Create a rate stream that generates one row
            rate_df = (self.spark.readStream
                    .format("rate")
                    .option("rowsPerSecond", 1)
                    .option("numPartitions", 1)
                    .load()
                    .limit(1))  # Only one dummy record
            
            if "PMU" in transaction_type:
                # PMU dummy record using rate stream
                dummy_df = rate_df.select(
                    lit(999999).alias("kafkaOffset"),
                    lit("dummy-pmu-activity").alias("kafkaTopic"),
                    lit("0").alias("kafkaPartition"),
                    lit("DUMMY000000").alias("acct_no"),
                    lit("20250101").alias("selling_date"),
                    lit("DUMMY000001").alias("ac_tran_no"),
                    lit(Decimal(0.00)).alias("ttl_cost"),
                    lit("20250101").alias("meeting_date"),
                    lit(1).alias("meeting_loc"),
                    lit("WIN").alias("bet_type"),
                    lit(1).alias("race_no"),
                    lit("ACP01").alias("oltp_id"),
                    lit("DUMMY001").alias("headerActivityID")
                )
                        
            elif "BET_FO" in transaction_type and "CANCEL" not in transaction_type:
                # BET_FO dummy record using rate stream
                dummy_df = rate_df.select(
                    lit(98793874).alias("kafkaOffset"),
                    lit("edap-s-ap-fo-activity").alias("kafkaTopic"),
                    lit("0").alias("kafkaPartition"),
                    create_map(lit("ActivityCode"), lit("1")).alias("headersRefined"),
                    lit(False).alias("is_error"),
                    lit(None).alias("error_details"),
                    lit("DUMMY000000").alias("header__accountNo"),
                    lit("000").alias("header__sellRequest__betVar__totalAmount"),
                    lit("DUMMY-12345").alias("ticket__ticketId"),
                    lit("DUMMY").alias("header__systemId"),
                    lit("WIN HV HV 01/01/2025").alias("header__betline"),
                    lit("1").alias("sport_id"),
                    lit("1").alias("activity_type"),
                    lit("2").alias("ticket_status")
                )
            
            elif "BET_FO_CANCEL" in transaction_type:
                # BET_FO_CANCEL with binary map
        
                dummy_df = rate_df.select(
                    lit(98793874).alias("kafkaOffset"),
                    lit("edap-s-ap-fo-activity").alias("kafkaTopic"),
                    lit("0").alias("kafkaPartition"),
                    create_map(lit("ActivityCode"), lit(bytearray(b"3"))).alias("headersRefined"),  # Binary map
                    lit(False).alias("is_error"),
                    lit(None).alias("error_details"),
                    lit("DUMMY000000").alias("header__accountNo"),
                    lit("000").alias("header__sellRequest__betVar__totalAmount"),
                    lit("DUMMY-12345").alias("ticket__ticketId"),
                    lit("DUMMY").alias("header__systemId"),
                    lit("WIN HV HV 01/01/2025").alias("header__betline"),
                    lit("1").alias("sport_id"),
                    lit("1").alias("activity_type"),
                    lit("2").alias("ticket_status")
                )
            
            # Add missing columns (same logic as before)
            for col_name in streaming_df.columns:
                if col_name not in dummy_df.columns:
                    dummy_df = dummy_df.withColumn(col_name, lit(None))
            
            # Ensure both DataFrames have same columns in same order
            all_columns = streaming_df.columns
            dummy_df = dummy_df.select(*all_columns)
            
            # Union (now both are streaming DataFrames)
            print("Starting Union")
            result_df = streaming_df.union(dummy_df)
            print(f"Successfully added dummy record for {transaction_type}")
            
            return result_df
            
        except Exception as e:
            print(f"ERROR in simple dummy record for {transaction_type}: {str(e)}")
            return streaming_df