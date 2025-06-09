"""OnboardDataflowSpec class provides bronze/silver onboarding features with enhanced parameterization support."""

import copy
import dataclasses
import json
import logging
import re
from typing import Dict, Any, List, Optional

import pyspark.sql.types as T
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from src.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils, SilverDataflowSpec
from src.metastore_ops import DeltaPipelinesInternalTableOps, DeltaPipelinesMetaStoreOps

logger = logging.getLogger("databricks.labs.dltmeta")
logger.setLevel(logging.INFO)


class OnboardDataflowspec:
    """OnboardDataflowSpec class provides bronze/silver onboarding features with enhanced parameterization."""

    def __init__(self, spark, dict_obj, bronze_schema_mapper=None, uc_enabled=False):
        """Onboard Dataflowspec Constructor."""
        self.spark = spark
        self.dict_obj = dict_obj
        self.bronze_dict_obj = copy.deepcopy(dict_obj)
        self.silver_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        self.env = dict_obj.get("env", "prod")  # Extract environment parameter
        self.__initialize_paths(uc_enabled)
        self.bronze_schema_mapper = bronze_schema_mapper
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.onboard_file_type = None

    def __initialize_paths(self, uc_enabled):
        """Initialize paths with proper cleanup."""
        if "silver_dataflowspec_table" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_table"]
        if "silver_dataflowspec_path" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_path"]

        if "bronze_dataflowspec_table" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_table"]
        if "bronze_dataflowspec_path" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_path"]
        if uc_enabled:
            print("uc_enabled:", uc_enabled)
            if "bronze_dataflowspec_path" in self.bronze_dict_obj:
                del self.bronze_dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.silver_dict_obj:
                del self.silver_dict_obj["silver_dataflowspec_path"]

    def substitute_environment_variables(self, data: Any, env: str) -> Any:
        """
        Recursively substitute environment variables in the data structure.
        
        Args:
            data: The data structure to process (dict, list, str, etc.)
            env: The environment value to substitute for {env} placeholders
            
        Returns:
            Data structure with substituted environment variables
        """
        if isinstance(data, dict):
            return {key: self.substitute_environment_variables(value, env) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.substitute_environment_variables(item, env) for item in data]
        elif isinstance(data, str):
            # Replace {env} placeholders with actual environment value
            return data.replace("{env}", env)
        else:
            return data

    def get_field_with_fallback(self, row_dict: Dict[str, Any], field_pattern: str, env: str) -> Any:
        """
        Get field value with fallback support for parameterized field names.
        
        Args:
            row_dict: Dictionary containing row data
            field_pattern: Field pattern (e.g., "silver_transformation_json_{env}")
            env: Environment value
            
        Returns:
            Field value if found, None otherwise
        """
        # Try exact field name with environment substitution
        exact_field = field_pattern.replace("{env}", env)
        if exact_field in row_dict:
            return row_dict[exact_field]
        
        # Try the original field pattern as-is
        if field_pattern in row_dict:
            return row_dict[field_pattern]
        
        # Try without environment suffix (fallback)
        base_field = re.sub(r'_{env}$', '', field_pattern).replace("{env}", "")
        if base_field in row_dict:
            return row_dict[base_field]
        
        return None

    def safe_field_access(self, row, field_name: str) -> Any:
        """
        Safely access a field from a PySpark Row with enhanced error handling.
        
        Args:
            row: PySpark Row object
            field_name: Name of the field to access
            
        Returns:
            Field value if exists, None otherwise
        """
        try:
            # Check if field exists in row
            if hasattr(row, '__fields__') and field_name in row.__fields__:
                return row[field_name]
            elif hasattr(row, 'asDict'):
                row_dict = row.asDict()
                return row_dict.get(field_name, None)
            else:
                return None
        except (ValueError, AttributeError, KeyError) as e:
            logger.warning(f"Field access failed for '{field_name}': {e}")
            return None

    def debug_row_fields(self, row, description: str = "Row"):
        """Debug helper to log available fields in a row."""
        try:
            if hasattr(row, '__fields__'):
                available_fields = row.__fields__
            elif hasattr(row, 'asDict'):
                available_fields = list(row.asDict().keys())
            else:
                available_fields = []
            
            logger.info(f"{description} available fields: {available_fields}")
        except Exception as e:
            logger.error(f"Failed to debug row fields: {e}")

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """Validate dict attributes method will validate dict attributes keys with enhanced error reporting."""
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            missing_attrs = set(attributes).difference(attributes_keys)
            extra_attrs = attributes_keys.difference(set(attributes))
            
            error_msg = f"Attribute validation failed:\n"
            if missing_attrs:
                error_msg += f"  Missing attributes: {missing_attrs}\n"
            if extra_attrs:
                error_msg += f"  Extra attributes: {extra_attrs}\n"
            
            logger.error(error_msg)
            raise ValueError(error_msg)

    def onboard_dataflow_specs(self):
        """
        Enhanced onboard_dataflow_specs method with better parameter validation.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        
        if self.uc_enabled:
            if "bronze_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["silver_dataflowspec_path"]
            self.__validate_dict_attributes(attributes, self.dict_obj)
        else:
            attributes.extend(["bronze_dataflowspec_path", "silver_dataflowspec_path"])
            self.__validate_dict_attributes(attributes, self.dict_obj)
        
        logger.info(f"Starting onboarding process for environment: {self.env}")
        self.onboard_bronze_dataflow_spec()
        self.onboard_silver_dataflow_spec()

    def register_bronze_dataflow_spec_tables(self):
        """Register bronze/silver dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["bronze_dataflowspec_table"],
            self.dict_obj["bronze_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded bronze table={self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        ).show()

    def register_silver_dataflow_spec_tables(self):
        """Register silver dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["silver_dataflowspec_table"],
            self.dict_obj["silver_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded silver table={self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        ).show()

    def onboard_silver_dataflow_spec(self):
        """
        Enhanced onboard silver dataflow spec with improved field handling.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.silver_dict_obj
        
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("silver_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        
        silver_data_flow_spec_df = self.__get_silver_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )
        
        columns = StructType(
            [
                StructField("select_exp", ArrayType(StringType(), True), True),
                StructField("target_partition_cols", ArrayType(StringType(), True), True),
                StructField("target_table", StringType(), True),
                StructField("where_clause", ArrayType(StringType(), True), True),
            ]
        )

        emp_rdd = []
        env = dict_obj["env"]
        silver_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )
        
        # Enhanced transformation JSON handling with environment substitution
        transformation_field = f"silver_transformation_json_{env}"
        silver_transformation_json_file = onboarding_df.select(transformation_field).dropDuplicates()

        silver_transformation_json_files = silver_transformation_json_file.collect()
        for row in silver_transformation_json_files:
            transformation_path = self.safe_field_access(row, transformation_field)
            if transformation_path:
                silver_transformation_json_df = silver_transformation_json_df.union(
                    self.spark.read.option("multiline", "true")
                    .schema(columns)
                    .json(transformation_path)
                )

        logger.info(f"Processing silver transformation files for environment: {env}")

        silver_data_flow_spec_df = silver_transformation_json_df.join(
            silver_data_flow_spec_df,
            silver_transformation_json_df.target_table
            == silver_data_flow_spec_df.targetDetails["table"],
        )
        
        silver_dataflow_spec_df = (
            silver_data_flow_spec_df.drop("target_table")
            .drop("target_partition_cols")
            .withColumnRenamed("select_exp", "selectExp")
            .withColumnRenamed("where_clause", "whereClause")
        )

        silver_dataflow_spec_df = self.__add_audit_columns(
            silver_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        silver_fields = [field.name for field in dataclasses.fields(SilverDataflowSpec)]
        silver_dataflow_spec_df = silver_dataflow_spec_df.select(silver_fields)
        database = dict_obj["database"]
        table = dict_obj["silver_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    silver_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                silver_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["silver_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["silver_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["silver_dataflowspec_path"]
                )
            logger.info("In Merge block for Silver")
            self.deltaPipelinesInternalTableOps.merge(
                silver_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_silver_dataflow_spec_tables()

    def onboard_bronze_dataflow_spec(self):
        """
        Enhanced onboard bronze dataflow spec with improved parameter handling.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.bronze_dict_obj
        
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("bronze_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )

        bronze_dataflow_spec_df = self.__get_bronze_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        bronze_dataflow_spec_df = self.__add_audit_columns(
            bronze_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        
        bronze_fields = [field.name for field in dataclasses.fields(BronzeDataflowSpec)]
        bronze_dataflow_spec_df = bronze_dataflow_spec_df.select(bronze_fields)
        database = dict_obj["database"]
        table = dict_obj["bronze_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    bronze_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                (
                    bronze_dataflow_spec_df.write.mode("overwrite")
                    .format("delta")
                    .option("mergeSchema", "true")
                    .save(path=dict_obj["bronze_dataflowspec_path"])
                )
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["bronze_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["bronze_dataflowspec_path"]
                )

            logger.info("In Merge block for Bronze")
            self.deltaPipelinesInternalTableOps.merge(
                bronze_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_bronze_dataflow_spec_tables()

    def __delete_none(self, _dict):
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def __get_onboarding_file_dataframe(self, onboarding_file_path):
        """Enhanced onboarding file processing with environment variable substitution."""
        onboarding_df = None
        if onboarding_file_path.lower().endswith(".json"):
            # Read the JSON file as text first for preprocessing
            json_content = self.spark.read.text(onboarding_file_path, wholetext=True).collect()[0]["value"]
            
            # Parse JSON and substitute environment variables
            json_data = json.loads(json_content)
            substituted_data = self.substitute_environment_variables(json_data, self.env)
            
            # Convert back to JSON string and create temporary file or use DataFrame
            substituted_json = json.dumps(substituted_data)
            
            # Create RDD from substituted JSON and convert to DataFrame
            json_rdd = self.spark.sparkContext.parallelize([substituted_json])
            onboarding_df = self.spark.read.json(json_rdd)
            
            onboarding_df.show(truncate=False)
            self.onboard_file_type = "json"
            
            # Check for duplicates
            onboarding_df_dupes = (
                onboarding_df.groupBy("data_flow_id").count().filter("count > 1")
            )
            if len(onboarding_df_dupes.head(1)) > 0:
                onboarding_df_dupes.show(truncate=False)
                raise Exception("onboarding file have duplicated data_flow_ids! ")
        else:
            raise Exception(
                "Onboarding file format not supported! Please provide json file format"
            )
        return onboarding_df

    def __add_audit_columns(self, df, dict_obj):
        """Add audit columns with enhanced validation."""
        attributes = ["import_author", "version"]
        self.__validate_dict_attributes(attributes, dict_obj)

        df = (
            df.withColumn("version", f.lit(dict_obj["version"]))
            .withColumn("createDate", f.current_timestamp())
            .withColumn("createdBy", f.lit(dict_obj["import_author"]))
            .withColumn("updateDate", f.current_timestamp())
            .withColumn("updatedBy", f.lit(dict_obj["import_author"]))
        )
        return df

    def __get_bronze_schema(self, metadata_file):
        """Get schema from metadata file in json format."""
        ddlSchemaStr = self.spark.read.text(
            paths=metadata_file, wholetext=True
        ).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        logger.info(spark_schema)
        schema = json.dumps(spark_schema.jsonValue())
        return schema

    def __validate_mandatory_fields(self, onboarding_row, mandatory_fields):
        """Enhanced mandatory field validation with better error reporting."""
        missing_fields = []
        row_dict = onboarding_row.asDict() if hasattr(onboarding_row, 'asDict') else {}
        
        logger.info(f"Validating fields: {mandatory_fields}")
        logger.info(f"Available fields: {list(row_dict.keys())}")
        
        for field in mandatory_fields:
            field_value = self.safe_field_access(onboarding_row, field)
            if field_value is None or (isinstance(field_value, str) and not field_value.strip()):
                missing_fields.append(field)
        
        if missing_fields:
            available_fields = list(row_dict.keys())
            error_msg = (
                f"Missing mandatory fields: {missing_fields}\n"
                f"Available fields: {available_fields}\n"
                f"Environment: {self.env}"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

    def __get_bronze_dataflow_spec_dataframe(self, onboarding_df, env):
        """Enhanced bronze dataflow spec creation with better field handling."""
        data_flow_spec_columns = [
            "dataFlowId", "dataFlowGroup", "sourceFormat", "sourceDetails",
            "readerConfigOptions", "targetFormat", "targetDetails", "tableProperties",
            "schema", "partitionColumns", "cdcApplyChanges", "applyChangesFromSnapshot",
            "dataQualityExpectations", "quarantineTargetDetails", "quarantineTableProperties",
            "appendFlows", "appendFlowsSchemas", "clusterBy", "targetPiiFields",
        ]
        
        data_flow_spec_schema = StructType([
            StructField("dataFlowId", StringType(), True),
            StructField("dataFlowGroup", StringType(), True),
            StructField("sourceFormat", StringType(), True),
            StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
            StructField("readerConfigOptions", MapType(StringType(), StringType(), True), True),
            StructField("targetFormat", StringType(), True),
            StructField("targetDetails", MapType(StringType(), StringType(), True), True),
            StructField("tableProperties", MapType(StringType(), StringType(), True), True),
            StructField("schema", StringType(), True),
            StructField("partitionColumns", ArrayType(StringType(), True), True),
            StructField("cdcApplyChanges", StringType(), True),
            StructField("applyChangesFromSnapshot", StringType(), True),
            StructField("dataQualityExpectations", StringType(), True),
            StructField("quarantineTargetDetails", MapType(StringType(), StringType(), True), True),
            StructField("quarantineTableProperties", MapType(StringType(), StringType(), True), True),
            StructField("appendFlows", StringType(), True),
            StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
            StructField("clusterBy", ArrayType(StringType(), True), True),
            StructField("targetPiiFields", MapType(StringType(), StringType(), True), True),
        ])
        
        data = []
        onboarding_rows = onboarding_df.collect()
        
        # Define mandatory fields with environment substitution
        mandatory_fields = [
            "data_flow_id", "data_flow_group", "source_details",
            f"bronze_database_{env}", "bronze_table", "bronze_reader_options",
        ]
        
        for onboarding_row in onboarding_rows:
            try:
                self.debug_row_fields(onboarding_row, "Bronze onboarding row")
                
                # Enhanced mandatory field validation
                extended_mandatory_fields = mandatory_fields.copy()
                if not self.uc_enabled:
                    extended_mandatory_fields.append(f"bronze_table_path_{env}")
                
                self.__validate_mandatory_fields(onboarding_row, extended_mandatory_fields)
                
                # Process row data with safe field access
                bronze_data_flow_spec_id = self.safe_field_access(onboarding_row, "data_flow_id")
                bronze_data_flow_spec_group = self.safe_field_access(onboarding_row, "data_flow_group")
                
                source_format = self.safe_field_access(onboarding_row, "source_format")
                if not source_format:
                    raise Exception(f"Source format not provided for row={onboarding_row}")

                if source_format.lower() not in ["cloudfiles", "eventhub", "kafka", "delta", "snapshot"]:
                    raise Exception(
                        f"Source format {source_format} not supported in DLT-META! row={onboarding_row}"
                    )
                
                source_details, bronze_reader_config_options, schema = (
                    self.get_bronze_source_details_reader_options_schema(onboarding_row, env)
                )
                
                bronze_target_format = "delta"
                bronze_target_details = {
                    "database": self.safe_field_access(onboarding_row, f"bronze_database_{env}"),
                    "table": self.safe_field_access(onboarding_row, "bronze_table"),
                }
                
                if not self.uc_enabled:
                    bronze_table_path = self.safe_field_access(onboarding_row, f"bronze_table_path_{env}")
                    if bronze_table_path:
                        bronze_target_details["path"] = bronze_table_path
                    else:
                        raise Exception(f"bronze_table_path_{env} not provided in onboarding_row")
                
                bronze_table_properties = {}
                table_props = self.safe_field_access(onboarding_row, "bronze_table_properties")
                if table_props:
                    bronze_table_properties = self.__delete_none(table_props.asDict())

                partition_columns = [""]
                partition_cols = self.safe_field_access(onboarding_row, "bronze_partition_columns")
                if partition_cols:
                    if "," in partition_cols:
                        partition_columns = partition_cols.split(",")
                    else:
                        partition_columns = [partition_cols]

                cluster_by = self.__get_cluster_by_properties(
                    onboarding_row, bronze_table_properties, "bronze_cluster_by"
                )

                cdc_apply_changes = None
                cdc_changes = self.safe_field_access(onboarding_row, "bronze_cdc_apply_changes")
                if cdc_changes:
                    self.__validate_apply_changes(onboarding_row, "bronze")
                    cdc_apply_changes = json.dumps(self.__delete_none(cdc_changes.asDict()))
                
                apply_changes_from_snapshot = None
                snapshot_changes = self.safe_field_access(onboarding_row, "bronze_apply_changes_from_snapshot")
                if snapshot_changes:
                    self.__validate_apply_changes_from_snapshot(onboarding_row, "bronze")
                    apply_changes_from_snapshot = json.dumps(
                        self.__delete_none(snapshot_changes.asDict())
                    )
                
                data_quality_expectations = None
                quarantine_target_details = {}
                quarantine_table_properties = {}
                
                dqe_field = f"bronze_data_quality_expectations_json_{env}"
                dqe_json = self.safe_field_access(onboarding_row, dqe_field)
                if dqe_json:
                    data_quality_expectations = self.__get_data_quality_expecations(dqe_json)
                    quarantine_table = self.safe_field_access(onboarding_row, "bronze_quarantine_table")
                    if quarantine_table:
                        quarantine_target_details, quarantine_table_properties = (
                            self.__get_quarantine_details(env, onboarding_row)
                        )
                
                targetPiiFields = {}
                pii_fields = self.safe_field_access(onboarding_row, "targetPiiFields")
                if pii_fields:
                    targetPiiFields = self.__delete_none(pii_fields.asDict())

                append_flows, append_flows_schemas = self.get_append_flows_json(
                    onboarding_row, "bronze", env
                )

                bronze_row = (
                    bronze_data_flow_spec_id, bronze_data_flow_spec_group, source_format,
                    source_details, bronze_reader_config_options, bronze_target_format,
                    bronze_target_details, bronze_table_properties, schema, partition_columns,
                    cdc_apply_changes, apply_changes_from_snapshot, data_quality_expectations,
                    quarantine_target_details, quarantine_table_properties, append_flows,
                    append_flows_schemas, cluster_by, targetPiiFields,
                )
                data.append(bronze_row)
                
            except Exception as e:
                logger.error(f"Error processing bronze row: {e}")
                self.debug_row_fields(onboarding_row, "Failed bronze row")
                raise

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __get_cluster_by_properties(self, onboarding_row, table_properties, cluster_key):
        """Enhanced cluster by properties handling."""
        cluster_by = None
        cluster_value = self.safe_field_access(onboarding_row, cluster_key)
        if cluster_value:
            if table_properties.get('pipelines.autoOptimize.zOrderCols', None) is not None:
                raise Exception(f"Can not support zOrder and cluster_by together at {cluster_key}")
            cluster_by = cluster_value
        return cluster_by

    def __get_quarantine_details(self, env, onboarding_row):
        """Enhanced quarantine details processing."""
        quarantine_table_partition_columns = ""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        
        partition_field = self.safe_field_access(onboarding_row, "bronze_quarantine_table_partitions")
        if partition_field:
            if "," in partition_field:
                quarantine_table_partition_columns = partition_field.split(",")
            else:
                quarantine_table_partition_columns = partition_field
        
        props_field = self.safe_field_access(onboarding_row, "bronze_quarantine_table_properties")
        if props_field:
            quarantine_table_properties = self.__delete_none(props_field.asDict())

        quarantine_table_cluster_by = self.__get_cluster_by_properties(
            onboarding_row, quarantine_table_properties, "bronze_quarantine_table_cluster_by"
        )
        
        quarantine_db_field = f"bronze_database_quarantine_{env}"
        quarantine_db = self.safe_field_access(onboarding_row, quarantine_db_field)
        quarantine_table = self.safe_field_access(onboarding_row, "bronze_quarantine_table")
        
        if quarantine_db and quarantine_table:
            quarantine_target_details = {
                "database": quarantine_db,
                "table": quarantine_table,
                "partition_columns": quarantine_table_partition_columns,
                "cluster_by": quarantine_table_cluster_by
            }
        
        if not self.uc_enabled:
            quarantine_path_field = f"bronze_quarantine_table_path_{env}"
            quarantine_path = self.safe_field_access(onboarding_row, quarantine_path_field)
            if quarantine_path:
                quarantine_target_details["path"] = quarantine_path

        return quarantine_target_details, quarantine_table_properties

    def get_append_flows_json(self, onboarding_row, layer, env):
        """Enhanced append flows processing with better field handling."""
        append_flows = None
        append_flows_schema = {}
        
        append_flows_field = f"{layer}_append_flows"
        append_flows_data = self.safe_field_access(onboarding_row, append_flows_field)
        
        if append_flows_data:
            self.__validate_append_flow(onboarding_row, layer)
            from pyspark.sql.types import Row

            af_list = []
            for json_append_flow in append_flows_data:
                json_append_flow = json_append_flow.asDict()
                append_flow_map = {}
                
                for key in json_append_flow.keys():
                    if isinstance(json_append_flow[key], Row):
                        fs = json_append_flow[key].__fields__
                        mp = {}
                        for ff in fs:
                            if f"source_path_{env}" == ff:
                                mp["path"] = json_append_flow[key][ff]
                            elif "source_schema_path" == ff:
                                source_schema_path = json_append_flow[key][ff]
                                if source_schema_path:
                                    schema = self.__get_bronze_schema(source_schema_path)
                                    append_flows_schema[json_append_flow["name"]] = schema
                            else:
                                mp[ff] = json_append_flow[key][ff]
                        append_flow_map[key] = self.__delete_none(mp)
                    else:
                        append_flow_map[key] = json_append_flow[key]
                af_list.append(self.__delete_none(append_flow_map))
            append_flows = json.dumps(af_list)
        
        return append_flows, append_flows_schema

    def __validate_apply_changes(self, onboarding_row, layer):
        """Enhanced apply changes validation."""
        cdc_field = f"{layer}_cdc_apply_changes"
        cdc_apply_changes = self.safe_field_access(onboarding_row, cdc_field)
        
        if not cdc_apply_changes:
            raise Exception(f"CDC apply changes field {cdc_field} not found in row")
        
        json_cdc_apply_changes = self.__delete_none(cdc_apply_changes.asDict())
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(
            DataflowSpecUtils.cdc_applychanges_api_attributes
        ).difference(payload_keys)
        
        logger.info(f"missing cdc payload keys:{missing_cdc_payload_keys} for row")
        
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(
                DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes
            ) - set(payload_keys)
            logger.error(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"mandatory missing attributes for {layer}_cdc_apply_changes = {missing_mandatory_attr}"
            )

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        """Enhanced apply changes from snapshot validation."""
        snapshot_field = f"{layer}_apply_changes_from_snapshot"
        apply_changes_from_snapshot = self.safe_field_access(onboarding_row, snapshot_field)
        
        if not apply_changes_from_snapshot:
            raise Exception(f"Apply changes from snapshot field {snapshot_field} not found in row")
        
        json_apply_changes_from_snapshot = self.__delete_none(apply_changes_from_snapshot.asDict())
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_keys = set(
            DataflowSpecUtils.apply_changes_from_snapshot_api_attributes
        ).difference(payload_keys)
        
        logger.info(f"missing applyChangesFromSnapshot payload keys:{missing_keys}")
        
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(
                DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes
            ) - set(payload_keys)
            logger.error(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"mandatory missing attributes for {layer}_apply_changes_from_snapshot = {missing_mandatory_attr}"
            )

    def get_bronze_source_details_reader_options_schema(self, onboarding_row, env):
        """Enhanced bronze source details processing."""
        source_details = {}
        bronze_reader_config_options = {}
        schema = None
        
        source_format = self.safe_field_access(onboarding_row, "source_format")
        reader_options = self.safe_field_access(onboarding_row, "bronze_reader_options")
        
        if reader_options:
            bronze_reader_config_options = self.__delete_none(reader_options.asDict())
        
        source_details_json = self.safe_field_access(onboarding_row, "source_details")
        if source_details_json:
            source_details_file = self.__delete_none(source_details_json.asDict())
            
            if source_format.lower() in ["cloudfiles", "delta", "snapshot"]:
                source_path_field = f"source_path_{env}"
                if source_path_field in source_details_file:
                    source_details["path"] = source_details_file[source_path_field]
                
                for field in ["source_database", "source_table"]:
                    if field in source_details_file:
                        source_details[field] = source_details_file[field]
                
                if "source_metadata" in source_details_file:
                    source_metadata_dict = self.__delete_none(
                        source_details_file["source_metadata"].asDict()
                    )
                    if "select_metadata_cols" in source_metadata_dict:
                        select_metadata_cols = self.__delete_none(
                            source_metadata_dict["select_metadata_cols"].asDict()
                        )
                        source_metadata_dict["select_metadata_cols"] = select_metadata_cols
                    source_details["source_metadata"] = json.dumps(
                        self.__delete_none(source_metadata_dict)
                    )
            
            if source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                
                source_path_field = f"source_path_{env}"
                if source_path_field in source_details_file:
                    source_details["path"] = source_details_file[source_path_field]
                else:
                    raise Exception(f"source_path_{env} is missing in the source_details")
            
            elif source_format.lower() in ["eventhub", "kafka"]:
                source_details = source_details_file
            
            if "source_schema_path" in source_details_file:
                source_schema_path = source_details_file["source_schema_path"]
                if source_schema_path:
                    if self.bronze_schema_mapper is not None:
                        schema = self.bronze_schema_mapper(source_schema_path, self.spark)
                    else:
                        schema = self.__get_bronze_schema(source_schema_path)
                else:
                    logger.info("no input schema provided for row")
                logger.info("spark_schema={}".format(schema))

        return source_details, bronze_reader_config_options, schema

    def __validate_append_flow(self, onboarding_row, layer):
        """Enhanced append flow validation."""
        append_flows_field = f"{layer}_append_flows"
        append_flows = self.safe_field_access(onboarding_row, append_flows_field)
        
        if not append_flows:
            return
        
        for append_flow in append_flows:
            json_append_flow = append_flow.asDict()
            logger.info(f"actual appendFlow={json_append_flow}")
            
            payload_keys = json_append_flow.keys()
            missing_keys = set(
                DataflowSpecUtils.append_flow_api_attributes_defaults
            ).difference(payload_keys)
            
            logger.info(f"missing append flow payload keys:{missing_keys}")
            
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = set(
                    DataflowSpecUtils.append_flow_mandatory_attributes
                ) - set(payload_keys)
                logger.error(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(
                    f"mandatory missing attributes for {layer}_append_flow = {missing_mandatory_attr}"
                )

    def __get_data_quality_expecations(self, json_file_path):
        """Enhanced data quality expectations processing."""
        json_string = None
        if json_file_path and json_file_path.endswith(".json"):
            expectations_df = self.spark.read.text(json_file_path, wholetext=True)
            expectations_arr = expectations_df.collect()
            if len(expectations_arr) == 1:
                json_string = expectations_df.collect()[0]["value"]
        return json_string

    def __get_silver_dataflow_spec_dataframe(self, onboarding_df, env):
        """Enhanced silver dataflow spec creation with improved field handling."""
        data_flow_spec_columns = [
            "dataFlowId", "dataFlowGroup", "sourceFormat", "sourceDetails",
            "readerConfigOptions", "targetFormat", "targetDetails", "tableProperties",
            "partitionColumns", "cdcApplyChanges", "dataQualityExpectations",
            "appendFlows", "appendFlowsSchemas", "clusterBy", "source_PiiFields", "target_PiiFields"
        ]
        
        data_flow_spec_schema = StructType([
            StructField("dataFlowId", StringType(), True),
            StructField("dataFlowGroup", StringType(), True),
            StructField("sourceFormat", StringType(), True),
            StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
            StructField("readerConfigOptions", MapType(StringType(), StringType(), True), True),
            StructField("targetFormat", StringType(), True),
            StructField("targetDetails", MapType(StringType(), StringType(), True), True),
            StructField("tableProperties", MapType(StringType(), StringType(), True), True),
            StructField("partitionColumns", ArrayType(StringType(), True), True),
            StructField("cdcApplyChanges", StringType(), True),
            StructField("dataQualityExpectations", StringType(), True),
            StructField("appendFlows", StringType(), True),
            StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
            StructField("clusterBy", ArrayType(StringType(), True), True),
            StructField("source_PiiFields", MapType(StringType(), StringType(), True), True),
            StructField("target_PiiFields", MapType(StringType(), StringType(), True), True)
        ])
        
        data = []
        onboarding_rows = onboarding_df.collect()
        
        mandatory_fields = [
            "data_flow_id", "data_flow_group",
            f"silver_database_{env}", "silver_table",
            f"silver_transformation_json_{env}",
        ]

        for onboarding_row in onboarding_rows:
            try:
                self.debug_row_fields(onboarding_row, "Silver onboarding row")
                
                extended_mandatory_fields = mandatory_fields.copy()
                if not self.uc_enabled:
                    extended_mandatory_fields.append(f"silver_table_path_{env}")
                
                self.__validate_mandatory_fields(onboarding_row, extended_mandatory_fields)
                
                silver_data_flow_spec_id = self.safe_field_access(onboarding_row, "data_flow_id")
                silver_data_flow_spec_group = self.safe_field_access(onboarding_row, "data_flow_group")
                silver_reader_config_options = {}

                silver_target_format = "delta"

                bronze_target_details = {
                    "database": self.safe_field_access(onboarding_row, f"bronze_database_{env}"),
                    "table": self.safe_field_access(onboarding_row, "bronze_table"),
                }
                
                silver_target_details = {
                    "database": self.safe_field_access(onboarding_row, f"silver_database_{env}"),
                    "table": self.safe_field_access(onboarding_row, "silver_table"),
                }

                if not self.uc_enabled:
                    bronze_path = self.safe_field_access(onboarding_row, f"bronze_table_path_{env}")
                    silver_path = self.safe_field_access(onboarding_row, f"silver_table_path_{env}")
                    
                    if bronze_path:
                        bronze_target_details["path"] = bronze_path
                    if silver_path:
                        silver_target_details["path"] = silver_path

                silver_table_properties = {}
                table_props = self.safe_field_access(onboarding_row, "silver_table_properties")
                if table_props:
                    silver_table_properties = self.__delete_none(table_props.asDict())

                silver_partition_columns = [""]
                partition_cols = self.safe_field_access(onboarding_row, "silver_partition_columns")
                if partition_cols:
                    if "," in partition_cols:
                        silver_partition_columns = partition_cols.split(",")
                    else:
                        silver_partition_columns = [partition_cols]

                silver_cluster_by = self.__get_cluster_by_properties(
                    onboarding_row, silver_table_properties, "silver_cluster_by"
                )

                silver_cdc_apply_changes = None
                cdc_changes = self.safe_field_access(onboarding_row, "silver_cdc_apply_changes")
                if cdc_changes:
                    self.__validate_apply_changes(onboarding_row, "silver")
                    if self.onboard_file_type == "json":
                        silver_cdc_apply_changes = json.dumps(
                            self.__delete_none(cdc_changes.asDict())
                        )
                
                data_quality_expectations = None
                dqe_field = f"silver_data_quality_expectations_json_{env}"
                dqe_json = self.safe_field_access(onboarding_row, dqe_field)
                if dqe_json:
                    data_quality_expectations = self.__get_data_quality_expecations(dqe_json)
                
                append_flows, append_flow_schemas = self.get_append_flows_json(
                    onboarding_row, layer="silver", env=env
                )

                source_PiiFields = {}
                source_pii = self.safe_field_access(onboarding_row, "source_PiiFields")
                if source_pii:
                    source_PiiFields = self.__delete_none(source_pii.asDict())
                
                target_PiiFields = {}
                target_pii = self.safe_field_access(onboarding_row, "target_PiiFields")
                if target_pii:
                    target_PiiFields = self.__delete_none(target_pii.asDict())

                silver_row = (
                    silver_data_flow_spec_id, silver_data_flow_spec_group, "delta",
                    bronze_target_details, silver_reader_config_options, silver_target_format,
                    silver_target_details, silver_table_properties, silver_partition_columns,
                    silver_cdc_apply_changes, data_quality_expectations, append_flows,
                    append_flow_schemas, silver_cluster_by, source_PiiFields, target_PiiFields
                )
                data.append(silver_row)
                logger.info(f"silver_data processed successfully")
                
            except Exception as e:
                logger.error(f"Error processing silver row: {e}")
                self.debug_row_fields(onboarding_row, "Failed silver row")
                raise

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        
        return data_flow_spec_rows_df