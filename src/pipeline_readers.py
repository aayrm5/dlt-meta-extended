"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import from_json, col, current_timestamp, udf, expr, map_from_entries, variant_get, parse_json, lit
from src.dataflow_spec import BronzeDataflowSpec
from src.dataflow_utils import DataflowUtils
import pyspark.sql.types as T

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)

def create_json_validation_udf(spark):
    """Create a JSON validation UDF"""
    
    # Define the UDF function inline to avoid serialization issues
    def validate_json_with_error(json_str):
        """Validate JSON string and return error details if invalid."""
        import json  # Import inside the function to ensure it's available
        try:
            if json_str is None:
                return (True, "JSON string is null")
            json.loads(json_str)
            return (False, None)
        except Exception as e:
            return (True, str(e))
    
    # Define schema
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType
    result_schema = StructType([
        StructField("is_error", BooleanType(), nullable=False),
        StructField("error_details", StringType(), nullable=True)
    ])
    
    # Create and register UDF
    validate_json_udf = udf(validate_json_with_error, result_schema)
    spark.udf.register("validate_json_udf", validate_json_with_error, result_schema)
    
    return validate_json_udf

def create_parse_message_udf(spark, is_cancel):
    """Create a parse message UDF that's self-contained"""

    from ab_translator.pmu_decoder_helper import MessageParser

    if is_cancel:
        parse_udf_cancel = MessageParser.get_udf(is_cancel=is_cancel)
        # spark.udf.register("parse_message_cancel_udf", parse_udf_cancel)    
        return parse_udf_cancel
    else:
        parse_udf = MessageParser.get_udf(is_cancel=is_cancel)
        # spark.udf.register("parse_message_udf", parse_udf)
        return parse_udf




class PipelineReaders:
    """PipelineReader Class.

    Returns:
        _type_: _description_
    """
    def __init__(self, spark, source_format, source_details, reader_config_options, dataflowSpec, schema_json=None, writer_config_options=None):
        """Init."""
        self.spark = spark
        self.source_format = source_format
        self.source_details = source_details
        self.reader_config_options = reader_config_options
        self.schema_json = schema_json
        self.writer_config_options = writer_config_options
        # Register the UDF with Spark session
        # self.spark.udf.register("validate_json_udf", validate_json_with_error, result_schema)

        self.validate_json_udf = create_json_validation_udf(spark)
        self.parse_message_udf = create_parse_message_udf(spark, is_cancel=False)
        self.parse_message_cancel_udf = create_parse_message_udf(spark, is_cancel=True)

        self.dataflowSpec = dataflowSpec
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec

    def read_dlt_cloud_files(self) -> DataFrame:
        """Read dlt cloud files.

        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        input_df = None
        source_path = self.source_details["path"]
        if self.schema_json and self.source_format != "delta":
            schema = StructType.fromJson(self.schema_json)
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .schema(schema)
                .load(source_path)
            )
        else:
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .load(source_path)
            )
        if self.source_details and "source_metadata" in self.source_details.keys():
            input_df = PipelineReaders.add_cloudfiles_metadata(self.source_details, input_df)
        if self.writer_config_options["includeIngestionTimeAsColumn"] == "true":
            input_df = input_df.withColumn("databricksIngestionTimestamp", current_timestamp())
        return input_df

    @staticmethod
    def add_cloudfiles_metadata(sourceDetails, input_df):
        source_metadata_json = json.loads(sourceDetails.get("source_metadata"))
        keys = source_metadata_json.keys()
        autoloader_metadata_column_flag = False
        source_metadata_col_name = "_metadata"
        input_df = input_df.selectExpr("*", f"{source_metadata_col_name}")
        if "select_metadata_cols" in source_metadata_json:
            select_metadata_cols = source_metadata_json["select_metadata_cols"]
            for select_metadata_col in select_metadata_cols:
                input_df = input_df.withColumn(select_metadata_col, col(select_metadata_cols[select_metadata_col]))
        if "include_autoloader_metadata_column" in keys:
            autoloader_metadata_column = source_metadata_json["include_autoloader_metadata_column"]
            autoloader_metadata_column_flag = True if autoloader_metadata_column.lower() == "true" else False
            if autoloader_metadata_column_flag and "autoloader_metadata_col_name" in source_metadata_json:
                custom_source_metadata_col_name = source_metadata_json["autoloader_metadata_col_name"]
                if custom_source_metadata_col_name != source_metadata_col_name:
                    input_df = input_df.withColumnRenamed(f"{source_metadata_col_name}",
                                                          f"{custom_source_metadata_col_name}")
            elif autoloader_metadata_column_flag and "autoloader_metadata_col_name" not in source_metadata_json:
                input_df = input_df.withColumnRenamed("_metadata", "source_metadata")
        else:
            input_df = input_df.drop(f"{source_metadata_col_name}")
        return input_df

    def read_dlt_delta(self) -> DataFrame:
        """Read dlt delta.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")

        if self.reader_config_options and len(self.reader_config_options) > 0:
            input_df = (self.spark.readStream.options(**self.reader_config_options).table(
                        f"""{self.source_details["source_database"]}
                            .{self.source_details["source_table"]}"""
                    )
            )
            
        else:
            input_df = (
                self.spark.readStream.table(
                    f"""{self.source_details["source_database"]}
                        .{self.source_details["source_table"]}"""
                )
            )
        if self.writer_config_options["includeIngestionTimeAsColumn"] == "true":
            input_df = input_df.withColumn("databricksIngestionTimestamp", current_timestamp())
        return input_df

    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark)

    def read_kafka(self) -> DataFrame:
        """Read eventhub with dataflowspec and schema.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        
        if "source_schema_path" in self.source_details:
            schema_path = self.source_details.get("source_schema_path")
            kafka_source_schema = DataflowUtils.get_bronze_schema(self, schema_path)

        if self.source_format == "eventhub":
            kafka_options = self.get_eventhub_kafka_options()
        elif self.source_format == "kafka":
            kafka_options = self.get_kafka_options()

        print("----------------Riyaz------------- " + str(kafka_options))

        keys_to_remove = {"custom_decode_fo", "custom_decode_pmu","custom_decode_trasaction_type","decode_headers"}
        custom_removed_kafka_options = {k: v for k, v in kafka_options.items() if k not in keys_to_remove}

        if "decode_headers" in kafka_options and kafka_options["decode_headers"] == "true":
            raw_df = (
                self.spark
                .readStream
                .format("kafka")
                .options(**custom_removed_kafka_options)
                .load()
                .withColumn("headers", expr("transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8') as value))"))
            )
        else:
            raw_df = (
                self.spark
                .readStream
                .format("kafka")
                .options(**custom_removed_kafka_options)
                .load()
            )

        if "custom_decode_fo" in kafka_options and kafka_options["custom_decode_fo"] == "true":
            print("----------------in custom decode FO-----------------------")

            if kafka_options["custom_decode_trasaction_type"] == "BET_FO":
                print("----------------In BET_FO-----------------------")
                raw_df = (raw_df
                    .withColumn("headers", expr("transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8') as value))"))      
                    .withColumn("decoded_value",expr("decode(value, 'utf-8')"))
                    .withColumn("validJson", expr("validate_json_udf(decoded_value)"))
                    .withColumn("jsonValue", from_json(col("decoded_value"),kafka_source_schema ))
                    .withColumn("headersRefined", expr("map_from_entries(headers)"))
                    .withColumn("kafkaMessageTimestamp", col("timestamp"))
                    .withColumn("kafkaOffset", col("offset"))
                    .withColumn("kafkaTopic", col('topic'))
                    .withColumn("kafkaPartition", col('partition'))
                    .withColumn("is_error", col("validJson.is_error"))
                    .withColumn("error_details", col("validJson.error_details"))
                    .withColumn("sport_id", variant_get(parse_json(col("decoded_value")), '$.ticket.sportId','string' ))
                    .withColumn('headeractivityType', variant_get(parse_json(col("decoded_value")), '$.activityType','string' ))
                    .withColumn('headerticketStatus', variant_get(parse_json(col("decoded_value")), '$.ticket.ticketStatus','string' ))
                    .withColumn("headerVersionNumber", expr("filter(headers, x -> x.key = 'VersionNumber')[0].value"))
                    .filter("decoded_value is not null and trim(decoded_value) != 'empty'")
                    .filter("sport_id = 1 and (headerticketStatus =2 or headeractivityType =1)")
                    .selectExpr("kafkaOffset","kafkaMessageTimestamp","kafkaTopic","kafkaPartition","headersRefined", "is_error","error_details", "jsonValue.*","sport_id","cast(headeractivityType as long)","cast(headerticketStatus as long)","cast(headerVersionNumber as long)")
                )
                if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
                    if isinstance(bronze_dataflow_spec.columnToExtract, list):
                        column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
                    else:
                        column_to_extract = bronze_dataflow_spec.columnToExtract or ""
                    raw_df = DataflowUtils.recurFlattenDF(raw_df,column_to_extract)

                raw_df = DataflowUtils.add_extra_record(self, raw_df, "BET_FO")
                
            elif kafka_options["custom_decode_trasaction_type"] == "BET_FO_CANCEL":
                print("----------------In BET_FO_CANCEL-----------------------")
                raw_df = (raw_df
                    .withColumn("headers", expr("transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8') as value))"))
                    .withColumn("decoded_value",expr("decode(value, 'utf-8')"))
                    .withColumn("validJson", expr("validate_json_udf(decoded_value)"))
                    .withColumn("jsonValue", from_json(col("decoded_value"),kafka_source_schema ))
                    .withColumn("headersRefined", expr("map_from_entries(headers)"))
                    .withColumn("kafkaMessageTimestamp", col("timestamp"))
                    .withColumn("kafkaOffset", col("offset"))
                    .withColumn("kafkaTopic", col('topic'))
                    .withColumn("kafkaPartition", col('partition'))
                    .withColumn("is_error", col("validJson.is_error"))
                    .withColumn("error_details", col("validJson.error_details"))
                    .withColumn("sport_id", variant_get(parse_json(col("decoded_value")), '$.ticket.sportId','string' ))
                    .withColumn('headeractivityType', variant_get(parse_json(col("decoded_value")), '$.activityType','string' ))
                    .withColumn('headerticketStatus', variant_get(parse_json(col("decoded_value")), '$.ticket.ticketStatus','string' ))
                    .withColumn("headerVersionNumber", expr("filter(headers, x -> x.key = 'VersionNumber')[0].value"))
                    .filter("decoded_value is not null and trim(decoded_value) != 'empty'")
                    .filter("sport_id = 1 and (headerticketStatus =5 or headeractivityType =3)")
                    .selectExpr("kafkaOffset","kafkaMessageTimestamp","kafkaTopic","kafkaPartition","headersRefined", "is_error","error_details", "jsonValue.*","sport_id","cast(headeractivityType as long)","cast(headerticketStatus as long)","cast(headerVersionNumber as long)")
                )
                if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
                    if isinstance(bronze_dataflow_spec.columnToExtract, list):
                        column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
                    else:
                        column_to_extract = bronze_dataflow_spec.columnToExtract or ""
                    raw_df = DataflowUtils.recurFlattenDF(raw_df,column_to_extract)

                raw_df = DataflowUtils.add_extra_record(self, raw_df, "BET_FO_CANCEL")

        elif "custom_decode_pmu" in kafka_options and kafka_options["custom_decode_pmu"] == "true":
            print("----------------in custom decode PMU-----------------------")

            if kafka_options["custom_decode_trasaction_type"] == "RACE_PMU":
                print("----------------In RACE_PMU-----------------------")
                raw_df = (raw_df
                    .withColumn("headers", expr("transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8') as value))"))
                    .withColumn("headersRefined", expr("map_from_entries(headers)"))
                    .withColumn("kafkaOffset", expr("offset"))
                    .withColumn("kafkaPartition", expr("partition"))
                    .withColumn("kafkaMessageTimestamp", expr("timestamp"))
                    .withColumn("kafkaTopic", expr("topic"))
                    .filter("headersRefined.ActivityCode = '6'")
                )
                raw_df = raw_df.selectExpr("parse_message_udf(hex(value)) as root", "kafkaTopic", "kafkaPartition", "kafkaOffset", "kafkaMessageTimestamp","headers")
                raw_df = raw_df.selectExpr("kafkaOffset", "kafkaTopic", "kafkaPartition", "kafkaMessageTimestamp", "root.errors.*", "root.headerFields.*", "root.valueFields.*", "headers")
                raw_df = raw_df.withColumn("msg_order_no", expr("filter(headers, x -> x.key = 'ActivityId')[0].value")).drop("headers")
                if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
                    if isinstance(bronze_dataflow_spec.columnToExtract, list):
                        column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
                    else:
                        column_to_extract = bronze_dataflow_spec.columnToExtract or ""
                    raw_df = DataflowUtils.recurFlattenDF(raw_df,column_to_extract)
                raw_df = DataflowUtils.add_extra_record(self, raw_df, "RACE_PMU")
                
            elif kafka_options["custom_decode_trasaction_type"] == "RACE_PMU_CANCEL":
                print("----------------In RACE_PMU_CANCEL-----------------------")
                raw_df = (raw_df
                    .withColumn("headers", expr("transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8') as value))"))
                    .withColumn("headersRefined", expr("map_from_entries(headers)"))
                    .withColumn("kafkaOffset", expr("offset"))
                    .withColumn("kafkaPartition", expr("partition"))
                    .withColumn("kafkaMessageTimestamp", expr("timestamp"))
                    .withColumn("kafkaTopic", expr("topic"))
                    .filter("headersRefined.ActivityCode = '8'")
                )
                raw_df = raw_df.selectExpr("parse_message_cancel_udf(hex(value)) as root", "kafkaTopic", "kafkaPartition", "kafkaOffset", "kafkaMessageTimestamp","headers")
                raw_df = raw_df.selectExpr("kafkaOffset", "kafkaTopic", "kafkaPartition", "kafkaMessageTimestamp", "root.errors.*", "root.headerFields.*", "root.valueFields.*", "headers")
                raw_df = raw_df.withColumn("msg_order_no", expr("filter(headers, x -> x.key = 'ActivityId')[0].value")).drop("headers")
                if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
                    if isinstance(bronze_dataflow_spec.columnToExtract, list):
                        column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
                    else:
                        column_to_extract = bronze_dataflow_spec.columnToExtract or ""
                    raw_df = DataflowUtils.recurFlattenDF(raw_df,column_to_extract)
                raw_df = DataflowUtils.add_extra_record(self, raw_df, "RACE_PMU_CANCEL")

        if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
            if isinstance(bronze_dataflow_spec.columnToExtract, list):
                column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
            else:
                column_to_extract = bronze_dataflow_spec.columnToExtract or ""
            raw_df = DataflowUtils.recurFlattenDF(raw_df,column_to_extract)

        if self.writer_config_options["includeIngestionTimeAsColumn"] == "true":
            raw_df = raw_df.withColumn("databricksIngestionTimestamp", current_timestamp())

        return raw_df

    def get_eventhub_kafka_options(self):
        """Get eventhub options from dataflowspec."""
        dbutils = self.get_db_utils()
        eh_namespace = self.source_details.get("eventhub.namespace")
        eh_port = self.source_details.get("eventhub.port")
        eh_name = self.source_details.get("eventhub.name")
        eh_shared_key_name = self.source_details.get("eventhub.accessKeyName")
        secret_name = self.source_details.get("eventhub.accessKeySecretName")
        if not secret_name:
            # set default value if "eventhub.accessKeySecretName" is not specified
            secret_name = eh_shared_key_name
        secret_scope = self.source_details.get("eventhub.secretsScopeName")
        eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
        eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
        eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
        eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

        eh_conn_options = {
            "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
            "subscribe": eh_name,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": sasl_config
        }
        kafka_options = {**eh_conn_options, **self.reader_config_options}
        return kafka_options

    def get_kafka_options(self):
        """Get kafka options from dataflowspec."""
        kafka_base_ops = {
            "kafka.bootstrap.servers": self.source_details.get("kafka.bootstrap.servers"),
            "subscribe": self.source_details.get("subscribe"),
            "kafka.security.protocol": self.source_details.get("kafka.security.protocol")
        }
        ssl_truststore_location = self.source_details.get("kafka.ssl.truststore.location", None)
        ssl_keystore_location = self.source_details.get("kafka.ssl.keystore.location", None)
        if ssl_truststore_location and ssl_keystore_location:
            truststore_scope = self.source_details.get("kafka.ssl.truststore.secrets.scope", None)
            truststore_key = self.source_details.get("kafka.ssl.truststore.secrets.key", None)
            keystore_scope = self.source_details.get("kafka.ssl.keystore.secrets.scope", None)
            keystore_key = self.source_details.get("kafka.ssl.keystore.secrets.key", None)
            if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                dbutils = self.get_db_utils()
                kafka_ssl_conn = {
                    "kafka.ssl.truststore.location": ssl_truststore_location,
                    "kafka.ssl.keystore.location": ssl_keystore_location,
                    "kafka.ssl.keystore.password": dbutils.secrets.get(keystore_scope, keystore_key),
                    "kafka.ssl.truststore.password": dbutils.secrets.get(truststore_scope, truststore_key)
                }
                kafka_options = {**kafka_base_ops, **kafka_ssl_conn, **self.reader_config_options}
            else:
                params = ["kafka.ssl.truststore.secrets.scope",
                          "kafka.ssl.truststore.secrets.key",
                          "kafka.ssl.keystore.secrets.scope",
                          "kafka.ssl.keystore.secrets.key"
                          ]
                raise Exception(f"Kafka ssl required params are: {params}! provided options are :{self.source_details}")
        else:
            kafka_options = {**kafka_base_ops, **self.reader_config_options}
        # print("Riyaz-------" + str(kafka_options))
        return kafka_options








