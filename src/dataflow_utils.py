"""Dataflow utility functions for shared functionality."""
import logging
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.functions import explode, explode_outer, col, current_timestamp, lit, create_map, to_timestamp
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
            
            if "PMU" in transaction_type and "CANCEL" not in transaction_type:
                # PMU dummy record using rate stream
                dummy_df = rate_df.select(
                    # Kafka metadata fields
                    lit(999999).alias("kafkaOffset"),
                    lit("dummy-pmu-activity").alias("kafkaTopic"),
                    lit(0).alias("kafkaPartition"),
                    current_timestamp().alias("KafkaTimeStamp"),
                    lit(False).alias("is_error"),
                    lit(None).cast("string").alias("error_details"),
                    
                    # Header fields
                    lit(12345).cast("long").alias("headerSystemID"),
                    lit("20250101").alias("headerBusinessDate"),
                    lit(999001).cast("long").alias("headerActivityID"),
                    lit(0).cast("long").alias("headerEnquiryStatus"),
                    lit(1).cast("long").alias("headerActivityNature"),
                    lit(0).cast("long").alias("headerErrorCode"),
                    lit(0).cast("long").alias("headerMessageCode"),
                    
                    # Business identifier fields
                    lit("ACP01").alias("oltp_id"),
                    lit(1).cast("long").alias("msg_order_no"),
                    to_timestamp(lit("2025-01-01 10:00:00"), "yyyy-MM-dd HH:mm:ss").alias("selling_date"),
                    lit(1024).cast("long").alias("msg_size"),
                    lit(100).cast("long").alias("msg_code"),
                    lit(0).cast("long").alias("err_code"),
                    lit(0).cast("long").alias("bcs_trap_msg_code"),
                    
                    # Staff and terminal fields
                    lit(12345).cast("long").alias("staff_no"),
                    lit(1).cast("long").alias("logical_term_no"),
                    lit(1000001).cast("long").alias("acct_no"),
                    lit(1).cast("long").alias("acct_file_file_no"),
                    lit(1).cast("long").alias("acct_file_block_no"),
                    lit(0).cast("long").alias("overflow_block_no"),
                    lit(0).cast("long").alias("offset_to_acct_unit"),
                    lit(999001).cast("long").alias("ac_tran_no"),
                    current_timestamp().alias("time_stamp"),
                    
                    # System control fields
                    lit(1).cast("long").alias("last_log_seq"),
                    lit(1).cast("long").alias("msn"),
                    lit(0).cast("long").alias("ext_req_type"),
                    lit(0).cast("long").alias("prev_txn_catch_up"),
                    lit(0).cast("long").alias("bt_exception"),
                    lit(0).cast("long").alias("msg_to_other_system"),
                    lit(0).cast("long").alias("pre_logon_flag"),
                    lit(0).cast("long").alias("ext_req_timeout_flag"),
                    lit(0).cast("long").alias("late_reply_flag"),
                    lit(0).cast("long").alias("upd_bcsmsg_flag"),
                    lit(0).cast("long").alias("upd_rcvmsg_flag"),
                    lit(0).cast("long").alias("overflow_required_flag"),
                    lit(0).cast("long").alias("cb_local_acct_release_flag"),
                    lit(0).cast("long").alias("no_flush_acct_release_flag"),
                    lit(0).cast("long").alias("training_acct"),
                    lit(0).cast("long").alias("acct_sess_info_append"),
                    
                    # Source and terminal identification
                    lit(1).cast("long").alias("source_type"),
                    lit(1).cast("long").alias("front_end_no"),
                    lit("DUMMY001").alias("v_term_no"),
                    lit(1).cast("long").alias("v_location_id"),
                    lit(1).cast("long").alias("d_cit_no"),
                    lit(1).cast("long").alias("d_pseudo_term_no"),
                    lit(1).cast("long").alias("d_frontend_no"),
                    lit("CIT-3").alias("cit_type"),
                    
                    # CB/BT center information
                    lit(123).cast("long").alias("cbbt_centre_no"),
                    lit(1).cast("long").alias("cbbt_window_no"),
                    lit(1).cast("long").alias("cbbt_logical_term_no"),
                    lit(1).cast("long").alias("cbbt_system_no"),
                    lit(123).cast("long").alias("old_cb_centre_no"),
                    lit(1).cast("long").alias("old_cb_window_no"),
                    lit(1).cast("long").alias("old_cb_channel_no"),
                    lit(1).cast("long").alias("old_cb_system_no"),
                    
                    # Policy and material fields
                    lit(1).cast("long").alias("pol_file_no"),
                    lit(1).cast("long").alias("pol_offset_no"),
                    lit("MAT001").alias("mat_no"),
                    lit(0).cast("long").alias("batch_deposit"),
                    lit(1).cast("long").alias("call_seq"),
                    lit(0).cast("long").alias("opt_mode"),
                    
                    # Racing/meeting information
                    to_timestamp(lit("2025-01-01"), "yyyy-MM-dd").alias("meeting_date"),
                    lit("1").alias("meeting_loc"),  # This matches the string type in schema
                    lit("WED").alias("meeting_day"),
                    lit(100.0).alias("ttl_pay"),
                    lit(10).cast("long").alias("unit_bet"),
                    lit(100.0).alias("ttl_cost"),
                    to_timestamp(lit("2025-01-01 09:30:00"), "yyyy-MM-dd HH:mm:ss").alias("sell_time"),
                    lit("WIN").alias("bet_type"),
                    lit("N").alias("cancel_flag"),
                    
                    # Allup fields (event 1)
                    lit(1).cast("long").alias("allup_event_no"),
                    lit("FORMULA1").alias("allup_formula"),
                    lit("WIN").alias("allup_pool_type1"),
                    lit("1").alias("allup_race_no1"),
                    lit(0).cast("long").alias("allup_banker_flag1"),
                    lit(0).cast("long").alias("allup_field_flag1"),
                    lit(0).cast("long").alias("allup_multi_flag1"),
                    lit(0).cast("long").alias("allup_multi_banker_flag1"),
                    lit(0).cast("long").alias("allup_random_flag1"),
                    lit(1).cast("long").alias("allup_no_of_combination1"),
                    lit(1.0).alias("allup_pay_factor1"),
                    
                    # Allup fields (event 2)
                    lit(None).cast("string").alias("allup_pool_type2"),
                    lit(None).cast("string").alias("allup_race_no2"),
                    lit(0).cast("long").alias("allup_banker_flag2"),
                    lit(0).cast("long").alias("allup_field_flag2"),
                    lit(0).cast("long").alias("allup_multi_flag2"),
                    lit(0).cast("long").alias("allup_multi_banker_flag2"),
                    lit(0).cast("long").alias("allup_random_flag2"),
                    lit(0).cast("long").alias("allup_no_of_combination2"),
                    lit(0.0).alias("allup_pay_factor2"),
                    
                    # Allup fields (event 3)
                    lit(None).cast("string").alias("allup_pool_type3"),
                    lit(None).cast("string").alias("allup_race_no3"),
                    lit(0).cast("long").alias("allup_banker_flag3"),
                    lit(0).cast("long").alias("allup_field_flag3"),
                    lit(0).cast("long").alias("allup_multi_flag3"),
                    lit(0).cast("long").alias("allup_multi_banker_flag3"),
                    lit(0).cast("long").alias("allup_random_flag3"),
                    lit(0).cast("long").alias("allup_no_of_combination3"),
                    lit(0.0).alias("allup_pay_factor3"),
                    
                    # Allup fields (event 4)
                    lit(None).cast("string").alias("allup_pool_type4"),
                    lit(None).cast("string").alias("allup_race_no4"),
                    lit(0).cast("long").alias("allup_banker_flag4"),
                    lit(0).cast("long").alias("allup_field_flag4"),
                    lit(0).cast("long").alias("allup_multi_flag4"),
                    lit(0).cast("long").alias("allup_multi_banker_flag4"),
                    lit(0).cast("long").alias("allup_random_flag4"),
                    lit(0).cast("long").alias("allup_no_of_combination4"),
                    lit(0.0).alias("allup_pay_factor4"),
                    
                    # Allup fields (event 5)
                    lit(None).cast("string").alias("allup_pool_type5"),
                    lit(None).cast("string").alias("allup_race_no5"),
                    lit(0).cast("long").alias("allup_banker_flag5"),
                    lit(0).cast("long").alias("allup_field_flag5"),
                    lit(0).cast("long").alias("allup_multi_flag5"),
                    lit(0).cast("long").alias("allup_multi_banker_flag5"),
                    lit(0).cast("long").alias("allup_random_flag5"),
                    lit(0).cast("long").alias("allup_no_of_combination5"),
                    lit(0.0).alias("allup_pay_factor5"),
                    
                    # Allup fields (event 6)
                    lit(None).cast("string").alias("allup_pool_type6"),
                    lit(None).cast("string").alias("allup_race_no6"),
                    lit(0).cast("long").alias("allup_banker_flag6"),
                    lit(0).cast("long").alias("allup_field_flag6"),
                    lit(0).cast("long").alias("allup_multi_flag6"),
                    lit(0).cast("long").alias("allup_multi_banker_flag6"),
                    lit(0).cast("long").alias("allup_random_flag6"),
                    lit(0).cast("long").alias("allup_no_of_combination6"),
                    lit(0.0).alias("allup_pay_factor6"),
                    
                    # Race and betting details
                    lit("1").alias("race_no"),
                    lit(0).cast("long").alias("banker_flag"),
                    lit(0).cast("long").alias("field_flag"),
                    lit(0).cast("long").alias("multiple_flag"),
                    lit(0).cast("long").alias("multi_banker_flag"),
                    lit(0).cast("long").alias("random_flag"),
                    lit("1,2,3").alias("sb_selection"),
                    
                    # Bitmap fields
                    lit(0).cast("long").alias("no_banker_bitmap1"),
                    lit(0).cast("long").alias("no_banker_bitmap2"),
                    lit(0).cast("long").alias("no_banker_bitmap3"),
                    lit("001100110011").alias("bitmap1"),
                    lit("000000000000").alias("bitmap2"),
                    lit("000000000000").alias("bitmap3"),
                    lit("000000000000").alias("bitmap4"),
                    lit("000000000000").alias("bitmap5"),
                    lit("000000000000").alias("bitmap6"),
                    
                    # Additional flags
                    lit(0).cast("long").alias("cross_selling_flag"),
                    lit(0).cast("long").alias("flexi_bet_flag"),
                    lit(1).cast("long").alias("no_of_combinations"),
                    lit(0).cast("long").alias("is_anonymous_acc"),
                    lit(0).cast("long").alias("is_csc_card")
                )

            elif "RACE_PMU_CANCEL" in transaction_type:
                # PMU Cancel Dummy Record
                dummy_df = rate_df.select(
                    # Kafka metadata fields
                    lit(999999).alias("kafkaOffset"),
                    lit("dummy-pmu-cancel-activity").alias("kafkaTopic"),
                    lit(0).alias("kafkaPartition"),
                    current_timestamp().alias("KafkaTimeStamp"),
                    lit(False).alias("is_error"),
                    lit(None).cast("string").alias("error_details"),
                    
                    # Header fields
                    lit(12345).cast("long").alias("headerSystemID"),
                    lit("20250101").alias("headerBusinessDate"),
                    lit(999002).cast("long").alias("headerActivityID"),
                    lit(0).cast("long").alias("headerEnquiryStatus"),
                    lit(2).cast("long").alias("headerActivityNature"),  # Different for cancel
                    lit(0).cast("long").alias("headerErrorCode"),
                    lit(0).cast("long").alias("headerMessageCode"),
                    
                    # Business identifier fields
                    lit("ACP01").alias("oltp_id"),
                    lit(2).cast("long").alias("msg_order_no"),
                    to_timestamp(lit("2025-01-01 10:30:00"), "yyyy-MM-dd HH:mm:ss").alias("selling_date"),
                    lit(1024).cast("long").alias("msg_size"),
                    lit(200).cast("long").alias("msg_code"),  # Different code for cancel
                    lit(0).cast("long").alias("err_code"),
                    lit(0).cast("long").alias("bcs_trap_msg_code"),
                    
                    # Staff and terminal fields
                    lit(12345).cast("long").alias("staff_no"),
                    lit(1).cast("long").alias("logical_term_no"),
                    lit(1000001).cast("long").alias("acct_no"),
                    lit(1).cast("long").alias("acct_file_file_no"),
                    lit(1).cast("long").alias("acct_file_block_no"),
                    lit(0).cast("long").alias("overflow_block_no"),
                    lit(0).cast("long").alias("offset_to_acct_unit"),
                    lit(999002).cast("long").alias("ac_tran_no"),
                    current_timestamp().alias("time_stamp"),
                    
                    # System control fields
                    lit(2).cast("long").alias("last_log_seq"),
                    lit(2).cast("long").alias("msn"),
                    lit(0).cast("long").alias("ext_req_type"),
                    lit(0).cast("long").alias("prev_txn_catch_up"),
                    lit(0).cast("long").alias("bt_exception"),
                    lit(0).cast("long").alias("msg_to_other_system"),
                    lit(0).cast("long").alias("pre_logon_flag"),
                    lit(0).cast("long").alias("ext_req_timeout_flag"),
                    lit(0).cast("long").alias("late_reply_flag"),
                    lit(0).cast("long").alias("upd_bcsmsg_flag"),
                    lit(0).cast("long").alias("upd_rcvmsg_flag"),
                    lit(0).cast("long").alias("overflow_required_flag"),
                    lit(0).cast("long").alias("cb_local_acct_release_flag"),
                    lit(0).cast("long").alias("no_flush_acct_release_flag"),
                    lit(0).cast("long").alias("training_acct"),
                    lit(0).cast("long").alias("acct_sess_info_append"),
                    
                    # Source and terminal identification
                    lit(1).cast("long").alias("source_type"),
                    lit(1).cast("long").alias("front_end_no"),
                    lit("DUMMY002").alias("v_term_no"),
                    lit(1).cast("long").alias("v_location_id"),
                    lit(1).cast("long").alias("d_cit_no"),
                    lit(1).cast("long").alias("d_pseudo_term_no"),
                    lit(1).cast("long").alias("d_frontend_no"),
                    lit("CIT-3").alias("cit_type"),
                    
                    # CB/BT center information
                    lit(123).cast("long").alias("cbbt_centre_no"),
                    lit(1).cast("long").alias("cbbt_window_no"),
                    lit(1).cast("long").alias("cbbt_logical_term_no"),
                    lit(1).cast("long").alias("cbbt_system_no"),
                    lit(123).cast("long").alias("old_cb_centre_no"),
                    lit(1).cast("long").alias("old_cb_window_no"),
                    lit(1).cast("long").alias("old_cb_channel_no"),
                    lit(1).cast("long").alias("old_cb_system_no"),
                    
                    # Policy and material fields
                    lit(1).cast("long").alias("pol_file_no"),
                    lit(1).cast("long").alias("pol_offset_no"),
                    lit("MAT002").alias("mat_no"),
                    lit(0).cast("long").alias("batch_deposit"),
                    lit(2).cast("long").alias("call_seq"),
                    lit(0).cast("long").alias("opt_mode"),
                    
                    # Cancellation specific fields
                    lit(1).cast("long").alias("trnx_cancel"),  # 1 = cancel transaction
                    lit(201).cast("long").alias("cancel_txn_code"),
                    lit(1).cast("long").alias("acct_file_file_no2"),
                    lit(1).cast("long").alias("acct_file_block_no2"),
                    lit(0).cast("long").alias("offset_to_acct_unit2"),
                    lit(0).cast("long").alias("cancel_on_other_unit"),
                    lit(0).cast("long").alias("cancel_earlier_call"),
                    lit(0).cast("long").alias("cancel_by_tsn"),
                    
                    # Lottery fields
                    lit(0).cast("long").alias("ltry_idx"),
                    lit(0).cast("long").alias("ltry_err_selection"),
                    lit(0).cast("long").alias("ltry_offset_no"),
                    lit(0).cast("long").alias("ltry_sell_src"),
                    lit(0).cast("long").alias("ltry_draw_year"),
                    lit(0).cast("long").alias("ltry_draw_no"),
                    lit(0).cast("long").alias("ltry_draw_type"),
                    lit(0.0).alias("ltry_unit_bet"),
                    lit(0.0).alias("ltry_ttl_cost"),
                    
                    # Meeting/Racing fields (for cancelled racing bet)
                    lit(1).cast("long").alias("meet_idx"),
                    lit(0).cast("long").alias("meet_err_race_no"),
                    lit(0).cast("long").alias("meet_err_selection"),
                    lit(100).cast("long").alias("meet_bet_offset_in_file"),
                    lit(1).cast("long").alias("meet_sell_src"),
                    to_timestamp(lit("2025-01-01"), "yyyy-MM-dd").alias("meet_date"),
                    lit(1).cast("long").alias("meet_loc"),  # Happy Valley
                    lit(3).cast("long").alias("meet_day"),  # Wednesday
                    lit(1).cast("long").alias("meet_type"),
                    lit(10.0).alias("meet_unit_bet"),
                    lit(100.0).alias("meet_ttl_cost"),  # Amount being cancelled
                    
                    # Withdrawal fields
                    lit(0.0).alias("withdraw_amt"),
                    lit(0.0).alias("withdraw_service_chrg"),
                    lit(0).cast("long").alias("withdraw_type"),
                    lit(0).cast("long").alias("withdraw_activated_by"),
                    lit(0).cast("long").alias("withdraw_src_type"),
                    lit(0).cast("long").alias("withdraw_cancel_flag"),
                    
                    # Sports betting fields
                    lit(0).cast("long").alias("sb_sell_src"),
                    lit(0).cast("long").alias("unit_bet"),
                    lit(0.0).alias("total_cost"),
                    lit(None).cast("timestamp").alias("sb_selling_time"),
                    lit(0).cast("long").alias("sb_bet_type"),
                    
                    # Deposit fields
                    lit(None).cast("timestamp").alias("dep_holdtime"),
                    lit(0.0).alias("dep_amt"),
                    lit(0.0).alias("dep_service_chrg"),
                    lit(0).cast("long").alias("dep_type"),
                    lit(0).cast("long").alias("dep_withholdable_flag"),
                    lit(0).cast("long").alias("dep_cancel_flag"),
                    lit(0).cast("long").alias("dep_reverse_flag"),
                    lit(0).cast("long").alias("dep_deposit_src"),
                    
                    # Multi-draw fields
                    lit(0).cast("long").alias("multi_draw_flag"),
                    lit(0).cast("long").alias("no_of_draw_selected"),
                    lit(0).cast("long").alias("no_of_draw_remain"),
                    lit(0).cast("long").alias("can_prev_day_flag"),
                    lit(None).cast("timestamp").alias("org_msg_business_date")
                )

                        
            elif "BET_FO" in transaction_type and "CANCEL" not in transaction_type:
                # BET_FO dummy record using rate stream
                dummy_df = rate_df.select(
                    # Kafka metadata fields
                    lit(999999).alias("kafkaOffset"),
                    current_timestamp().alias("kafkaMessageTimestamp"),
                    lit("dummy-fo-betting-activity").alias("kafkaTopic"),
                    lit(0).alias("kafkaPartition"),
                    
                    # Headers map field - key-value pairs
                    create_map(
                        lit("ActivityCode"), lit("1")
                    ).alias("headersRefined"),
                    
                    # Error handling fields
                    lit(False).alias("is_error"),
                    lit(None).cast("string").alias("error_details"),
                    
                    # Activity fields
                    lit(1).cast("long").alias("activityCode"),
                    lit(100001).cast("long").alias("activityId"),
                    lit("2025-01-01T10:00:00Z").alias("activityTime"),
                    lit(1).cast("long").alias("activityType"),
                    lit(1).cast("long").alias("messageType"),
                    lit(0).cast("long").alias("previousTicketStatus"),
                    lit(100001).cast("long").alias("sourceActivityId"),
                    lit(2).cast("long").alias("ticketStatusSnapshot"),
                    lit("TRACE-001").alias("traceabilityId"),
                    lit("1").alias("sport_id"),  # Racing
                    lit(1).cast("long").alias("headeractivityType"),
                    lit(2).cast("long").alias("headerticketStatus"),
                    lit(1).cast("long").alias("headerVersionNumber"),
                    
                    # Header account and transaction fields
                    lit(1).cast("long").alias("header__accountCategory"),
                    lit("1000001").alias("header__accountNo"),
                    lit(999001).cast("long").alias("header__accountTransactionNo"),
                    lit("HR TNC1 ST 26/11/2023*2[C Fownes]@3.75+1[A S Cruz]@3.25+14[K W Lui]@25 $50 $150.00").alias("header__betline"),
                    lit(False).alias("header__crossSellIndicator"),
                    lit(12345).cast("long").alias("header__customerSessionId"),
                    lit(1000).cast("long").alias("header__dailyFixedOddsInvestment"),
                    lit(20250101).cast("long").alias("header__dataGroupingBizdate"),
                    lit(None).cast("string").alias("header__errorText"),
                    lit(1).cast("long").alias("header__frontendNo"),
                    lit(50000).cast("long").alias("header__fundsAvailableForBetting"),
                    lit(False).alias("header__isPayoutMayExceedLimit"),
                    lit(False).alias("header__isTrainingAccount"),
                    lit(1).cast("long").alias("header__logicalTerminalId"),
                    lit(1).cast("long").alias("header__msn"),
                    lit(0).cast("long").alias("header__previousSellRequestStatus"),
                    lit(0).cast("long").alias("header__resultCode"),
                    lit(None).cast("string").alias("header__resultText"),
                    lit(2).cast("long").alias("header__sellRequestStatusSnapshot"),
                    lit(1).cast("long").alias("header__sourceType"),
                    lit("12345").alias("header__staffNo"),
                    lit(0).cast("long").alias("header__sysOfferCount"),
                    lit(1).cast("long").alias("header__systemId"),
                    lit(12345).cast("long").alias("header__terminalSessionId"),
                    
                    # Ticket fields
                    lit(1).cast("long").alias("ticket__accountClassId"),
                    lit(1).cast("long").alias("ticket__collationSequence"),
                    lit(1).cast("long").alias("ticket__collationType"),
                    lit(0).cast("long").alias("ticket__counterOfferType"),
                    lit(20250101).cast("long").alias("ticket__dataGroupingBizdate"),
                    lit(False).alias("ticket__isElite"),
                    lit(True).alias("ticket__isFixedOdds"),
                    lit(None).cast("long").alias("ticket__originalTicketId"),
                    lit(999001).cast("long").alias("ticket__sellRequestId"),
                    lit("2025-01-01T10:00:00").alias("ticket__sellingDateTime"),
                    lit(1).cast("long").alias("ticket__sportId"),
                    lit(999001).cast("long").alias("ticket__ticketId"),
                    lit(2).cast("long").alias("ticket__ticketStatus"),
                    lit(999001).cast("long").alias("ticket__uniqueTicketId"),
                    
                    # Processing location fields
                    lit(1).cast("long").alias("header__processingLocation__processingLocationType"),
                    
                    # Sell request fields
                    lit(0).cast("long").alias("header__sellRequest__counterOfferType"),
                    lit(True).alias("header__sellRequest__isFixedOdds"),
                    lit(None).cast("string").alias("header__sellRequest__listBetInterceptReasons"),
                    lit(None).cast("string").alias("header__sellRequest__listCounterOffers"),
                    lit(None).cast("long").alias("header__sellRequest__parentSellRequestId"),
                    lit(999001).cast("long").alias("header__sellRequest__sellRequestId"),
                    lit(2).cast("long").alias("header__sellRequest__sellRequestStatus"),
                    lit(1).cast("long").alias("header__sellRequest__sportId"),
                    
                    # Ticket bet variation fields
                    lit(1).cast("long").alias("ticket__betVar__noOfBet"),
                    lit(10000).cast("long").alias("ticket__betVar__totalAmount"),  # $100.00 in cents
                    lit(35000).cast("long").alias("ticket__betVar__totalPayout"),   # $350.00 potential payout
                    lit(0).cast("long").alias("ticket__betVar__totalRefund"),
                    lit(10000).cast("long").alias("ticket__betVar__unitAmount"),
                    
                    # CB Location fields
                    lit("123").alias("header__processingLocation__cbLocation__centreNo"),
                    lit("1").alias("header__processingLocation__cbLocation__escNo"),
                    lit("N").alias("header__processingLocation__cbLocation__isHoTerminal"),
                    lit("N").alias("header__processingLocation__cbLocation__isOnCourse"),
                    lit("N").alias("header__processingLocation__cbLocation__isSvt"),
                    lit("1000").alias("header__processingLocation__cbLocation__minTicketCostAmount"),
                    lit("FO").alias("header__processingLocation__cbLocation__sourceSystemId"),
                    lit("001").alias("header__processingLocation__cbLocation__terminalNo"),
                    lit("1").alias("header__processingLocation__cbLocation__windowNo"),
                    
                    # Direct input location fields
                    lit(1).cast("long").alias("header__processingLocation__directInputLocation__citNo"),
                    lit(17).cast("long").alias("header__processingLocation__directInputLocation__citType"),
                    lit(1).cast("long").alias("header__processingLocation__directInputLocation__pseudoTerminalNo"),
                    
                    # Voice location fields
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__locationId"),
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__terminalNo"),
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__trackId"),
                    
                    # Header sell request bet variation fields
                    lit(1).cast("long").alias("header__sellRequest__betVar__noOfBet"),
                    lit(10000).cast("long").alias("header__sellRequest__betVar__totalAmount"),
                    lit(10000).cast("long").alias("header__sellRequest__betVar__unitAmount"),
                    
                    # All up fields for ticket
                    lit(None).cast("long").alias("ticket__betVar__allUp__allUpLevelNo"),
                    
                    # List legs fields for ticket
                    lit(1).cast("long").alias("ticket__betVar__listLegs__emsSequence"),
                    lit(12345).cast("long").alias("ticket__betVar__listLegs__eventId"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__eventOperationStatus"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__financialIndex"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__legOrder"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__poolId"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__raceNo"),
                    lit(12345).cast("long").alias("ticket__betVar__listLegs__tagEventId"),
                    
                    # All up fields for header sell request
                    lit(None).cast("long").alias("header__sellRequest__betVar__allUp__allUpLevelNo"),
                    lit(None).cast("string").alias("header__sellRequest__betVar__allUp__formula"),
                    
                    # List legs fields for header sell request
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__emsSequence"),
                    lit(12345).cast("long").alias("header__sellRequest__betVar__listLegs__eventId"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__eventOperationStatus"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__financialIndex"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__legOrder"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__poolId"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__raceNo"),
                    lit(12345).cast("long").alias("header__sellRequest__betVar__listLegs__tagEventId"),
                    
                    # List combinations fields for ticket
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__combinationId"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__combinationOrder"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__emsSequence"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__lineId"),
                    lit(3.5).alias("ticket__betVar__listLegs__listCombinations__odds"),
                    
                    # List combinations fields for header sell request
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__combinationId"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__combinationOrder"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__emsSequence"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__lineId"),
                    lit(3.5).alias("header__sellRequest__betVar__listLegs__listCombinations__odds")
                )
            
            elif "BET_FO_CANCEL" in transaction_type:
                # BET_FO_CANCEL with binary map
                dummy_df = rate_df.select(
                    # Kafka metadata fields
                    lit(999999).alias("kafkaOffset"),
                    current_timestamp().alias("kafkaMessageTimestamp"),
                    lit("dummy-fo-cancel-activity").alias("kafkaTopic"),
                    lit(0).alias("kafkaPartition"),
                    
                    # Headers map field with BINARY values (key difference from bet schema)
                    create_map(
                        lit("ActivityCode"), lit("3")  # Binary value for cancel
                    ).alias("headersRefined"),
                    
                    # Error handling fields
                    lit(False).alias("is_error"),
                    lit(None).cast("string").alias("error_details"),
                    
                    # Activity fields - cancel-specific values
                    lit(3).cast("long").alias("activityCode"),  # Cancel activity
                    lit(100002).cast("long").alias("activityId"),
                    lit("2025-01-01T10:30:00Z").alias("activityTime"),
                    lit(3).cast("long").alias("activityType"),  # Cancel type
                    lit(2).cast("long").alias("messageType"),
                    lit(2).cast("long").alias("previousTicketStatus"),  # Was accepted
                    lit(100002).cast("long").alias("sourceActivityId"),
                    lit(5).cast("long").alias("ticketStatusSnapshot"),  # Cancelled status
                    lit("TRACE-002").alias("traceabilityId"),
                    lit("1").alias("sport_id"),  # Racing
                    lit(3).cast("long").alias("headeractivityType"),
                    lit(5).cast("long").alias("headerticketStatus"),
                    lit(1).cast("long").alias("headerVersionNumber"),
                    
                    # Header account and transaction fields
                    lit(1).cast("long").alias("header__accountCategory"),
                    lit("1000001").alias("header__accountNo"),
                    lit(999002).cast("long").alias("header__accountTransactionNo"),
                    lit("HR TNC1 ST 26/11/2023*2[C Fownes]@3.75+1[A S Cruz]@3.25+14[K W Lui]@25 $50 $150.00").alias("header__betline"),
                    lit(False).alias("header__crossSellIndicator"),
                    lit(12345).cast("long").alias("header__customerSessionId"),
                    lit(1000).cast("long").alias("header__dailyFixedOddsInvestment"),
                    lit(20250101).cast("long").alias("header__dataGroupingBizdate"),
                    lit(None).cast("string").alias("header__errorText"),
                    lit(1).cast("long").alias("header__frontendNo"),
                    lit(50000).cast("long").alias("header__fundsAvailableForBetting"),
                    lit(False).alias("header__isPayoutMayExceedLimit"),
                    lit(False).alias("header__isTrainingAccount"),
                    lit(1).cast("long").alias("header__logicalTerminalId"),
                    lit(2).cast("long").alias("header__msn"),  # Different for cancel
                    lit(2).cast("long").alias("header__previousSellRequestStatus"),
                    lit(0).cast("long").alias("header__resultCode"),
                    lit("CANCELLED").alias("header__resultText"),
                    lit(5).cast("long").alias("header__sellRequestStatusSnapshot"),  # Cancelled
                    lit(1).cast("long").alias("header__sourceType"),
                    lit("12345").alias("header__staffNo"),
                    lit(0).cast("long").alias("header__sysOfferCount"),
                    lit(1).cast("long").alias("header__systemId"),
                    lit(12345).cast("long").alias("header__terminalSessionId"),
                    
                    # Ticket fields
                    lit(1).cast("long").alias("ticket__accountClassId"),
                    lit(2).cast("long").alias("ticket__collationSequence"),  # Cancel sequence
                    lit(2).cast("long").alias("ticket__collationType"),     # Cancel type
                    lit(0).cast("long").alias("ticket__counterOfferType"),
                    lit(20250101).cast("long").alias("ticket__dataGroupingBizdate"),
                    lit(False).alias("ticket__isElite"),
                    lit(True).alias("ticket__isFixedOdds"),
                    lit(999001).cast("long").alias("ticket__originalTicketId"),  # Original ticket being cancelled
                    lit(999002).cast("long").alias("ticket__sellRequestId"),
                    lit("2025-01-01T10:30:00").alias("ticket__sellingDateTime"),
                    lit(1).cast("long").alias("ticket__sportId"),
                    lit(999002).cast("long").alias("ticket__ticketId"),
                    lit(5).cast("long").alias("ticket__ticketStatus"),  # Cancelled
                    lit(999002).cast("long").alias("ticket__uniqueTicketId"),
                    
                    # Processing location fields
                    lit(1).cast("long").alias("header__processingLocation__processingLocationType"),
                    
                    # Sell request fields
                    lit(0).cast("long").alias("header__sellRequest__counterOfferType"),
                    lit(True).alias("header__sellRequest__isFixedOdds"),
                    lit(None).cast("string").alias("header__sellRequest__listBetInterceptReasons"),
                    lit(None).cast("string").alias("header__sellRequest__listCounterOffers"),
                    lit(999001).cast("long").alias("header__sellRequest__parentSellRequestId"),  # Original request
                    lit(999002).cast("long").alias("header__sellRequest__sellRequestId"),
                    lit(5).cast("long").alias("header__sellRequest__sellRequestStatus"),  # Cancelled
                    lit(1).cast("long").alias("header__sellRequest__sportId"),
                    
                    # Ticket bet variation fields - refund amounts for cancellation
                    lit(1).cast("long").alias("ticket__betVar__noOfBet"),
                    lit(0).cast("long").alias("ticket__betVar__totalAmount"),     # No new amount
                    lit(0).cast("long").alias("ticket__betVar__totalPayout"),    # No payout
                    lit(10000).cast("long").alias("ticket__betVar__totalRefund"), # $100.00 refunded
                    lit(0).cast("long").alias("ticket__betVar__unitAmount"),
                    
                    # CB Location fields
                    lit("123").alias("header__processingLocation__cbLocation__centreNo"),
                    lit("1").alias("header__processingLocation__cbLocation__escNo"),
                    lit("N").alias("header__processingLocation__cbLocation__isHoTerminal"),
                    lit("N").alias("header__processingLocation__cbLocation__isOnCourse"),
                    lit("N").alias("header__processingLocation__cbLocation__isSvt"),
                    lit("1000").alias("header__processingLocation__cbLocation__minTicketCostAmount"),
                    lit("FO").alias("header__processingLocation__cbLocation__sourceSystemId"),
                    lit("001").alias("header__processingLocation__cbLocation__terminalNo"),
                    lit("1").alias("header__processingLocation__cbLocation__windowNo"),
                    
                    # Direct input location fields
                    lit(1).cast("long").alias("header__processingLocation__directInputLocation__citNo"),
                    lit(17).cast("long").alias("header__processingLocation__directInputLocation__citType"),
                    lit(1).cast("long").alias("header__processingLocation__directInputLocation__pseudoTerminalNo"),
                    
                    # Voice location fields
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__locationId"),
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__terminalNo"),
                    lit(None).cast("string").alias("header__processingLocation__voiceLocation__trackId"),
                    
                    # Header sell request bet variation fields
                    lit(1).cast("long").alias("header__sellRequest__betVar__noOfBet"),
                    lit(0).cast("long").alias("header__sellRequest__betVar__totalAmount"),  # Cancel - no amount
                    lit(0).cast("long").alias("header__sellRequest__betVar__unitAmount"),
                    
                    # All up fields for ticket
                    lit(None).cast("long").alias("ticket__betVar__allUp__allUpLevelNo"),
                    
                    # List legs fields for ticket
                    lit(1).cast("long").alias("ticket__betVar__listLegs__emsSequence"),
                    lit(12345).cast("long").alias("ticket__betVar__listLegs__eventId"),
                    lit(3).cast("long").alias("ticket__betVar__listLegs__eventOperationStatus"),  # Cancelled
                    lit(1).cast("long").alias("ticket__betVar__listLegs__financialIndex"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__legOrder"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__poolId"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__raceNo"),
                    lit(12345).cast("long").alias("ticket__betVar__listLegs__tagEventId"),
                    
                    # All up fields for header sell request
                    lit(None).cast("long").alias("header__sellRequest__betVar__allUp__allUpLevelNo"),
                    lit(None).cast("string").alias("header__sellRequest__betVar__allUp__formula"),
                    
                    # List legs fields for header sell request
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__emsSequence"),
                    lit(12345).cast("long").alias("header__sellRequest__betVar__listLegs__eventId"),
                    lit(3).cast("long").alias("header__sellRequest__betVar__listLegs__eventOperationStatus"),  # Cancelled
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__financialIndex"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__legOrder"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__poolId"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__raceNo"),
                    lit(12345).cast("long").alias("header__sellRequest__betVar__listLegs__tagEventId"),
                    
                    # List combinations fields for ticket
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__combinationId"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__combinationOrder"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__emsSequence"),
                    lit(1).cast("long").alias("ticket__betVar__listLegs__listCombinations__lineId"),
                    lit(3.5).alias("ticket__betVar__listLegs__listCombinations__odds"),  # Original odds
                    
                    # List combinations fields for header sell request
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__combinationId"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__combinationOrder"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__emsSequence"),
                    lit(1).cast("long").alias("header__sellRequest__betVar__listLegs__listCombinations__lineId"),
                    lit(3.5).alias("header__sellRequest__betVar__listLegs__listCombinations__odds")  # Original odds
                )
            
            # Ensure both DataFrames have same columns in same order
            # all_columns = streaming_df.columns
            # dummy_df = dummy_df.select(*all_columns)

            print(dummy_df.printSchema())
            
            # Union (now both are streaming DataFrames)
            print("Starting Union")
            result_df = streaming_df.union(dummy_df)
            print(f"----------===========Successfully added dummy record for {transaction_type}============-----------------")
            
            return result_df
            
        except Exception as e:
            print(f"*********************ERROR in dummy record addition for {transaction_type}: {str(e)}*****************************ERROR")
            return streaming_df
        


