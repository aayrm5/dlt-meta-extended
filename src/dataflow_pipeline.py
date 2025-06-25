"""DataflowPipeline provide generic DLT code using dataflowspec."""
import json
import logging
import dlt
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import expr
from src.__about__ import __version__
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, GoldDataflowSpec, DataflowSpecUtils
from src.pipeline_readers import PipelineReaders
from src.ab_cancel_translator_integration import ABCancelTranslatorPipeline
from src.dataflow_pipeline_gold import GoldDataflowPipeline,GoldSourceProcessingUtils,GoldDltViewUtils
from src.dataflow_utils import DataflowUtils

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class AppendFlowWriter:
    """Append Flow Writer class."""

    def __init__(self, spark, append_flow, target, struct_schema, table_properties=None, partition_cols=None,
                 cluster_by=None):
        """Init."""
        self.spark = spark
        self.target = target
        self.append_flow = append_flow
        self.struct_schema = struct_schema
        self.table_properties = table_properties
        self.partition_cols = partition_cols
        self.cluster_by = cluster_by

    def write_af_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(f"{self.append_flow.name}_view")

    def write_flow(self):
        """Write Append Flow. 
        Append Flow's target has to be a 3 level namespace fully qualified target table"""
        if self.append_flow.create_streaming_table:
            dlt.create_streaming_table(
                name=self.target,
                table_properties=self.table_properties,
                partition_cols=DataflowSpecUtils.get_partition_cols(self.partition_cols),
                cluster_by=DataflowSpecUtils.get_partition_cols(self.cluster_by),
                schema=self.struct_schema,
                expect_all=None,
                expect_all_or_drop=None,
                expect_all_or_fail=None,
            )
        if self.append_flow.comment:
            comment = self.append_flow.comment
        else:
            comment = f"append_flow={self.append_flow.name} for target={self.target}"
        dlt.append_flow(name=self.append_flow.name,
                        target=self.target,
                        comment=comment,
                        spark_conf=self.append_flow.spark_conf,
                        once=self.append_flow.once,
                        )(self.write_af_to_delta)


class DataflowPipeline:
    """This class uses dataflowSpec to launch DLT with centralized PII encryption/decryption support.

    Uses encryption/decryption functions from catalog_dlt_meta.default
    """

    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine=None,
                 custom_transform_func: Callable = None, next_snapshot_and_version: Callable = None):
        """Initialize Constructor."""
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        if isinstance(dataflow_spec, BronzeDataflowSpec) or isinstance(dataflow_spec, SilverDataflowSpec) or isinstance(dataflow_spec, GoldDataflowSpec):
            self.__initialize_dataflow_pipeline(
                spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
            )
        else:
            raise Exception("Dataflow not supported!")

    def __initialize_dataflow_pipeline(
        self, spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func: Callable,
        next_snapshot_and_version: Callable
    ):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        dbp_enabled_str = spark.conf.get("pipelines.schema", None)
        spark.conf.set("databrickslab.dlt-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dpm_enabled = True if dbp_enabled_str else False
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        self.custom_transform_func = custom_transform_func
        cdc_apply_changes = getattr(dataflow_spec, 'cdcApplyChanges', None)
        if cdc_apply_changes:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        append_flows = getattr(dataflow_spec, 'appendFlows', None)
        if append_flows:
            self.appendFlows = DataflowSpecUtils.get_append_flows(dataflow_spec.appendFlows)
        else:
            self.appendFlows = None
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            self.next_snapshot_and_version = next_snapshot_and_version
            if self.next_snapshot_and_version:
                self.appy_changes_from_snapshot = DataflowSpecUtils.get_apply_changes_from_snapshot(
                    self.dataflowSpec.applyChangesFromSnapshot
                )
            else:
                if dataflow_spec.sourceFormat == "snapshot":
                    raise Exception(f"Snapshot reader function not provided for dataflowspec={dataflow_spec}!")
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        else:
            self.schema_json = None
            self.next_snapshot_and_version = None
            self.appy_changes_from_snapshot = None
        if isinstance(dataflow_spec, GoldDataflowSpec):
            self.gold_schema = None
        else:
            self.silver_schema = None
        
        self.gold_pipeline = GoldDataflowPipeline(spark, dataflow_spec, view_name, view_name_quarantine, self.encryptDataset, self.decryptDataset)

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and not self.next_snapshot_and_version:
            dlt.view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec) and not self.next_snapshot_and_version:
            dlt.view(
                self.read_silver,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, GoldDataflowSpec) and not self.next_snapshot_and_version:
            gold_final_view = self.gold_pipeline.read()
            @dlt.view(name=self.view_name)
            def final_gold_view():
                return dlt.read_stream(gold_final_view)
        else:
            if not self.next_snapshot_and_version:
                raise Exception("Dataflow read not supported for {}".format(type(self.dataflowSpec)))
        if self.appendFlows:
            self.read_append_flows()

    def read_append_flows(self):
        if self.dataflowSpec.appendFlows:
            append_flows_schema_map = self.dataflowSpec.appendFlowsSchemas
            for append_flow in self.appendFlows:
                flow_schema = None
                if append_flows_schema_map:
                    flow_schema = append_flows_schema_map.get(append_flow.name)
                pipeline_reader = PipelineReaders(
                    self.spark,
                    append_flow.source_format,
                    append_flow.source_details,
                    append_flow.reader_options,
                    self.dataflowSpec,
                    json.loads(flow_schema) if flow_schema else None
                )
                if append_flow.source_format == "cloudFiles":
                    dlt.view(pipeline_reader.read_dlt_cloud_files,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "delta":
                    dlt.view(pipeline_reader.read_dlt_delta,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "eventhub" or append_flow.source_format == "kafka":
                    dlt.view(pipeline_reader.read_kafka,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
        else:
            raise Exception(f"Append Flows not found for dataflowSpec={self.dataflowSpec}")

    def write(self,view_name=None):
        """Write DLT."""
        if isinstance(self.dataflowSpec, BronzeDataflowSpec):
            self.write_bronze()
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            self.write_silver()
        elif isinstance(self.dataflowSpec, GoldDataflowSpec):
            self.write_gold()
        else:
            raise Exception(f"Dataflow write not supported for type= {type(self.dataflowSpec)}")

    def write_bronze(self):
        """Write Bronze tables."""
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        database_name = bronze_dataflow_spec.targetDetails['database']
        table_name = bronze_dataflow_spec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"
        if bronze_dataflow_spec.sourceFormat and bronze_dataflow_spec.sourceFormat.lower() == "snapshot":
            if self.next_snapshot_and_version:
                self.apply_changes_from_snapshot()
            else:
                raise Exception("Snapshot reader function not provided!")
        elif bronze_dataflow_spec.dataQualityExpectations:
            self.write_bronze_with_dqe()
        elif bronze_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronze_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=fully_qualified_table_name,
                partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.clusterBy),
                table_properties=bronze_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"bronze dlt table{fully_qualified_table_name}"
            )
        if bronze_dataflow_spec.appendFlows:
            self.write_append_flows()

    def write_silver(self):
        """Write silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec

        database_name = silver_dataflow_spec.targetDetails['database']
        table_name = silver_dataflow_spec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"

        if silver_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else silver_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=fully_qualified_table_name,
                partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.clusterBy),
                table_properties=silver_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"silver dlt table {fully_qualified_table_name}"
            )
        if silver_dataflow_spec.appendFlows:
            self.write_append_flows()

    def write_gold(self):
        """Write gold tables."""
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec

        database_name = gold_dataflow_spec.targetDetails['database']
        table_name = gold_dataflow_spec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"

        cdc_apply_changes = getattr(gold_dataflow_spec, 'cdcApplyChanges', None)
        if cdc_apply_changes:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else gold_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=fully_qualified_table_name,
                partition_cols=DataflowSpecUtils.get_partition_cols(gold_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(gold_dataflow_spec.clusterBy),
                table_properties=gold_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"Gold dlt table {fully_qualified_table_name}"
            )
        if gold_dataflow_spec.appendFlows:
            self.write_append_flows()

    def read_bronze(self) -> DataFrame:
        """Read Bronze Table with PII encryption and AB Cancel Translator support using centralized functions."""
        logger.info("In read_bronze func with centralized PII encryption and AB Cancel Translator support")
        
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        input_df = None

        pipeline_reader = PipelineReaders(
            self.spark,
            self.dataflowSpec.sourceFormat,
            self.dataflowSpec.sourceDetails,
            self.dataflowSpec.readerConfigOptions,
            self.dataflowSpec,
            self.schema_json,
            self.dataflowSpec.writerConfigOptions
        )

        if bronze_dataflow_spec.sourceFormat == "cloudFiles" and bronze_dataflow_spec.isStreaming == "true":
            input_df = self.read_source_streaming()

        elif (bronze_dataflow_spec.sourceFormat == "csv" or bronze_dataflow_spec.sourceFormat == "parquet" or bronze_dataflow_spec.sourceFormat == "json" or bronze_dataflow_spec.sourceFormat == "delta" ):
            if bronze_dataflow_spec.isStreaming == "false":
                input_df = self.read_source_batch()
            else:
                input_df = self.read_source_streaming()
            
        elif bronze_dataflow_spec.sourceFormat == "eventhub" or bronze_dataflow_spec.sourceFormat == "kafka":
            input_df = pipeline_reader.read_kafka()

        else:
            raise Exception(f"{bronze_dataflow_spec.sourceFormat} source format not supported")
        
        # Apply custom transformations
        input_df = self.apply_custom_transform_fun(input_df)
        
        # Apply PII encryption
        target_pii_fields = getattr(bronze_dataflow_spec, 'targetPiiFields', {})
        if target_pii_fields and len(target_pii_fields) > 0:
            logger.info(f"Applying PII encryption to bronze layer fields: {list(target_pii_fields.keys())}")
            input_df = self.encryptDataset(
                data=input_df,
                piiFields=target_pii_fields,
                df_spec=bronze_dataflow_spec
            )
            logger.info("PII encryption completed for bronze layer")
        else:
            logger.info("No PII fields configured for encryption in bronze layer")
        
        return input_df

    def get_silver_schema(self):
        """Get Silver table Schema."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
            path=silver_dataflow_spec.sourceDetails["path"],
            format="delta"
        ).selectExpr(*select_exp)
        raw_delta_table_stream = self.__apply_where_clause(where_clause, raw_delta_table_stream)
        return raw_delta_table_stream.schema

    def __apply_where_clause(self, where_clause, raw_delta_table_stream):
        """Apply where clause provided in silver transformations."""
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)
        return raw_delta_table_stream

    def read_silver(self) -> DataFrame:
        """Read Silver tables with PII decryption/encryption support using centralized functions."""
        logger.info("In read_silver func with centralized PII decryption/encryption")
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        
        # Get PII field configurations
        source_pii_fields = getattr(silver_dataflow_spec, 'source_PiiFields', {})
        target_pii_fields = getattr(silver_dataflow_spec, 'target_PiiFields', {})
        
        # Read source data from bronze layer 
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        ) if self.uc_enabled else self.spark.readStream.load(
            path=silver_dataflow_spec.sourceDetails["path"],
            format="delta"
        )
        
        # Decrypt source PII fields from bronze layer 
        decrypted_data = raw_delta_table_stream
        if source_pii_fields and len(source_pii_fields) > 0:
            logger.info(f"Decrypting source PII fields from bronze: {list(source_pii_fields.keys())}")
            decrypted_data = self.decryptDataset(
                data=raw_delta_table_stream,
                piiFields=source_pii_fields
            )
            logger.info("Source PII decryption completed")
        else:
            logger.info("No source PII fields configured for decryption")
        
        # Apply transformations 
        transformed_data = decrypted_data.selectExpr(*select_exp)
        transformed_data = self.__apply_where_clause(where_clause, transformed_data)
        
        # Apply custom transformations
        transformed_data = self.apply_custom_transform_fun(transformed_data)
        
        # Encrypt target PII fields for silver layer 
        final_data = transformed_data
        if target_pii_fields and len(target_pii_fields) > 0:
            logger.info(f"Applying PII encryption to silver layer fields: {list(target_pii_fields.keys())}")
            final_data = self.encryptDataset(
                data=transformed_data,
                piiFields=target_pii_fields,
                df_spec=silver_dataflow_spec
            )
            logger.info("Target PII encryption completed for silver layer")
        else:
            logger.info("No target PII fields configured for encryption in silver layer")
        
        return final_data
    
    def read_source_streaming(self) -> DataFrame:
        """Generic Method for to read raw data for autoloader based streaming paths for bronze tables
        Returns:
            dataframe to be persisted to bronze table"""
        logger.info("In read_source_streaming func")
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        source_path = bronze_dataflow_spec.sourceDetails["path"]
        reader_config_options = bronze_dataflow_spec.readerConfigOptions
        if self.schema_json:
            schema = StructType.fromJson(self.schema_json)

            dataframe = self.spark.readStream.format(bronze_dataflow_spec.sourceFormat) \
                    .options(**reader_config_options) \
                    .schema(schema) \
                    .load(source_path)
        else:
            dataframe = self.spark.readStream.format(bronze_dataflow_spec.sourceFormat)\
                    .options(**reader_config_options)\
                    .load(source_path)
        if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
            if isinstance(bronze_dataflow_spec.columnToExtract, list):
                column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
            else:
                column_to_extract = bronze_dataflow_spec.columnToExtract or ""
            dataframe = DataflowUtils.recurFlattenDF(dataframe, column_to_extract)
        return dataframe

    def read_source_batch(self) -> DataFrame:
        """Generic Method for to read raw data for CSV/Parquet/json/delta files for bronze tables
        Returns:
            dataframe to be persisted to bronze table"""
        logger.info("In read_source_batch func")
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        source_path = bronze_dataflow_spec.sourceDetails["path"]
        reader_config_options = bronze_dataflow_spec.readerConfigOptions
        if self.schema_json:
            schema = StructType.fromJson(self.schema_json)

            dataframe = self.spark.read.format(bronze_dataflow_spec.sourceFormat) \
                    .options(**reader_config_options) \
                    .schema(schema) \
                    .load(source_path)
        else:
            dataframe = self.spark.read.format(bronze_dataflow_spec.sourceFormat) \
                    .options(**reader_config_options) \
                    .load(source_path)
        if(bronze_dataflow_spec.flattenNestedData is not None and bronze_dataflow_spec.flattenNestedData == "true") :
            if isinstance(bronze_dataflow_spec.columnToExtract, list):
                column_to_extract = bronze_dataflow_spec.columnToExtract[0] if bronze_dataflow_spec.columnToExtract else ""
            else:
                column_to_extract = bronze_dataflow_spec.columnToExtract or ""
            dataframe = DataflowUtils.recurFlattenDF(dataframe, column_to_extract)
        return dataframe

    def write_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(self.view_name)
    
    def encryptDataset(self, data: DataFrame, piiFields: dict, df_spec=None) -> DataFrame:
        """
        Generic Method to Encrypt Dataset using centralized encryption functions at catalog_dlt_meta.default
        
        Args:
            data (DataFrame): Source Dataframe to be encrypted
            piiFields (dict): Dictionary of PII column name and data type
            df_spec: Dataflow specification object (optional)
        
        Returns:
            DataFrame: Encrypted dataframe
        """
        logger.info("Starting PII encryption process using catalog_dlt_meta.default.encryptDatabricks")
        
        # Clean column names
        columns = data.columns
        for colmn in columns:
            data = data.withColumnRenamed(existing=f"{colmn}", new=f"{colmn.strip()}")
        
        # Validate PII fields
        piiFieldsList = list(map(lambda x: x.lower(), piiFields.keys()))
        datasetColumns = list(map(lambda x: x.lower(), data.columns))
        missingColumns = list(set(piiFieldsList).difference(datasetColumns))
        
        logger.info(f"PII fields to encrypt: {piiFieldsList}")
        
        if len(piiFieldsList) == 0:
            logger.info("No PII fields to encrypt")
            return data
            
        if len(missingColumns) > 0:
            logger.warning(f"PII fields {missingColumns} not found in dataset columns {datasetColumns}")
            # Continue with available fields instead of failing
            available_pii_fields = {k: v for k, v in piiFields.items() if k.lower() not in [m.lower() for m in missingColumns]}
            if len(available_pii_fields) == 0:
                logger.warning("No PII fields available for encryption")
                return data
            piiFields = available_pii_fields
        
        # Apply encryption to each PII field using centralized functions
        for column in piiFields:
            datatype = piiFields[column]
            if datatype is not None:
                try:
                    # Handle complex data types
                    if datatype.lower().count("array") or datatype.lower().count("struct"):
                        data = data.withColumn(column, expr(f"to_json(`{column}`)"))
                    else:
                        data = data.withColumn(column, expr(f"cast(`{column}` as string)"))
                    
                    # Apply encryption using centralized function at catalog_dlt_meta.default
                    data = data.withColumn(column, expr(f"catalog_dlt_meta.default.encryptDatabricks(`{column}`)"))
                    logger.info(f"Encrypted PII field: {column}")
                    
                except Exception as e:
                    logger.error(f"Failed to encrypt field {column}: {e}")
                    # Continue with other fields
                    continue
        
        logger.info("PII encryption process completed using catalog_dlt_meta.default")
        return data

    def decryptDataset(self, data: DataFrame, piiFields: dict) -> DataFrame:
        """
        Generic Method to Decrypt Dataset using centralized decryption functions at catalog_dlt_meta.default
        
        Args:
            data (DataFrame): Source Dataframe to be decrypted
            piiFields (dict): Dictionary of PII column name and data type
        
        Returns:
            DataFrame: Decrypted dataframe
        """
        logger.info("Starting PII decryption process using catalog_dlt_meta.default.decryptDatabricks")
        
        # Validate PII fields
        piiFieldsList = list(map(lambda x: x.lower(), piiFields.keys()))
        datasetColumns = list(map(lambda x: x.lower(), data.columns))
        missingColumns = list(set(piiFieldsList).difference(datasetColumns))
        
        if len(piiFieldsList) == 0:
            logger.info("No PII fields to decrypt")
            return data
        
        if len(missingColumns) > 0:
            logger.warning(f"PII fields {missingColumns} not found in dataset columns {datasetColumns}")
            # Continue with available fields
            available_pii_fields = {k: v for k, v in piiFields.items() if k.lower() not in [m.lower() for m in missingColumns]}
            if len(available_pii_fields) == 0:
                logger.warning("No PII fields available for decryption")
                return data
            piiFields = available_pii_fields
        
        # Apply decryption to each PII field using centralized functions
        for column in piiFields:
            datatype = piiFields[column]
            if datatype is not None:
                try:
                    # Apply decryption using centralized function at catalog_dlt_meta.default
                    data = data.withColumn(column, expr(f"catalog_dlt_meta.default.decryptDatabricks({column})"))
                    logger.info(f"Decrypted PII field: {column}")
                    
                    # Handle complex data types
                    if datatype.lower().count("array") or datatype.lower().count("struct"):
                        data = data.withColumn(column, expr(f"from_json({column}, '{datatype}')"))
                    else:
                        data = data.withColumn(column, expr(f"cast({column} as {datatype})"))
                        
                except Exception as e:
                    logger.error(f"Failed to decrypt field {column}: {e}")
                    # Continue with other fields
                    continue
        
        logger.info("PII decryption process completed using catalog_dlt_meta.default")
        return data

    def apply_custom_transform_fun(self, input_df):
        if self.custom_transform_func:
            input_df = self.custom_transform_func(input_df, self.dataflowSpec)
        return input_df

    def apply_changes_from_snapshot(self):
        database_name = self.dataflowSpec.targetDetails['database']
        table_name = self.dataflowSpec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        self.create_streaming_table(None, target_path)
        dlt.apply_changes_from_snapshot(
            target=fully_qualified_table_name,
            source=lambda latest_snapshot_version:
            self.next_snapshot_and_version(latest_snapshot_version,
                                           self.dataflowSpec
                                           ),
            keys=self.appy_changes_from_snapshot.keys,
            stored_as_scd_type=self.appy_changes_from_snapshot.scd_type,
            track_history_column_list=self.appy_changes_from_snapshot.track_history_column_list,
            track_history_except_column_list=self.appy_changes_from_snapshot.track_history_except_column_list,
        )

    def write_bronze_with_dqe(self):
        """Write Bronze table with data quality expectations."""
        bronzeDataflowSpec: BronzeDataflowSpec = self.dataflowSpec

        database_name = bronzeDataflowSpec.targetDetails['database']
        table_name = bronzeDataflowSpec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"

        quarantine_database_name = bronzeDataflowSpec.quarantineTargetDetails['database']
        quarantine_table_name = bronzeDataflowSpec.quarantineTargetDetails['table']
        quarantine_fully_qualified_table_name = f"{quarantine_database_name}.{quarantine_table_name}"

        data_quality_expectations_json = json.loads(bronzeDataflowSpec.dataQualityExpectations)

        dlt_table_with_expectation = None
        expect_or_quarantine_dict = None
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        if bronzeDataflowSpec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronzeDataflowSpec.targetDetails["path"]
            if expect_all_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_all_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=fully_qualified_table_name,
                        table_properties=bronzeDataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                        path=target_path,
                        comment=f"bronze dlt table {fully_qualified_table_name}"
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=fully_qualified_table_name,
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table {fully_qualified_table_name}"
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt_table_with_expectation)
            if expect_all_or_drop_dict:
                if expect_all_dict is None and expect_all_or_fail_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=fully_qualified_table_name,
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table {fully_qualified_table_name}"
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt_table_with_expectation)
            if expect_or_quarantine_dict:
                q_partition_cols = None
                q_cluster_by = None
                if (
                    "partition_columns" in bronzeDataflowSpec.quarantineTargetDetails
                    and bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]
                ):
                    q_partition_cols = [bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]]

                if (
                        "cluster_by" in bronzeDataflowSpec.quarantineTargetDetails
                        and bronzeDataflowSpec.quarantineTargetDetails["cluster_by"]
                ):
                    q_cluster_by = DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.
                                                                        quarantineTargetDetails['cluster_by'])

                target_path = None if self.uc_enabled else bronzeDataflowSpec.quarantineTargetDetails["path"]
                dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=quarantine_fully_qualified_table_name,
                        table_properties=bronzeDataflowSpec.quarantineTableProperties,
                        partition_cols=q_partition_cols,
                        cluster_by=q_cluster_by,
                        path=target_path,
                        comment=f"""bronze dlt quarantine_path table
                        {quarantine_fully_qualified_table_name}""",
                    )
                )

    def write_append_flows(self):
        """Creates an append flow for the target specified in the dataflowSpec."""

        database_name = self.dataflowSpec.targetDetails['database']
        table_name = self.dataflowSpec.targetDetails['table']
        fully_qualified_table_name =  f"{database_name}.{table_name}"

        for append_flow in self.appendFlows:
            struct_schema = None
            if self.schema_json:
                struct_schema = (
                    StructType.fromJson(self.schema_json)
                    if isinstance(self.dataflowSpec, BronzeDataflowSpec)
                    else self.silver_schema
                )
            append_flow_writer = AppendFlowWriter(
                self.spark, 
                append_flow,
                fully_qualified_table_name,
                struct_schema,
                self.dataflowSpec.tableProperties,
                self.dataflowSpec.partitionColumns,
                self.dataflowSpec.clusterBy
            )
            append_flow_writer.write_flow()

    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        struct_schema = None
        if self.schema_json:
            struct_schema = self.modify_schema_for_cdc_changes(cdc_apply_changes)

        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        self.create_streaming_table(struct_schema, target_path)

        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        target_table = (
            f"{self.dataflowSpec.targetDetails['database']}.{self.dataflowSpec.targetDetails['table']}"
            if self.uc_enabled and self.dpm_enabled
            else self.dataflowSpec.targetDetails['table']
        )
        dlt.apply_changes(
            target=target_table,
            source=self.view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=cdc_apply_changes.sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list,
            flow_name=cdc_apply_changes.flow_name,
            once=cdc_apply_changes.once,
            ignore_null_updates_column_list=cdc_apply_changes.ignore_null_updates_column_list,
            ignore_null_updates_except_column_list=cdc_apply_changes.ignore_null_updates_except_column_list
        )

    def modify_schema_for_cdc_changes(self, cdc_apply_changes):
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.schema_json is None:
            return None
        if isinstance(self.dataflowSpec, SilverDataflowSpec) and self.silver_schema is None:
            return None

        struct_schema = (
            StructType.fromJson(self.schema_json)
            if isinstance(self.dataflowSpec, BronzeDataflowSpec)
            else self.silver_schema
        )

        sequenced_by_data_type = None

        if cdc_apply_changes.except_column_list:
            modified_schema = StructType([])
            if struct_schema:
                for field in struct_schema.fields:
                    if field.name not in cdc_apply_changes.except_column_list:
                        modified_schema.add(field)
                    if field.name == cdc_apply_changes.sequence_by:
                        sequenced_by_data_type = field.dataType
                struct_schema = modified_schema
            else:
                raise Exception(f"Schema is None for {self.dataflowSpec} for cdc_apply_changes! ")

        if struct_schema and cdc_apply_changes.scd_type == "2":
            struct_schema.add(StructField("__START_AT", sequenced_by_data_type))
            struct_schema.add(StructField("__END_AT", sequenced_by_data_type))
        return struct_schema

    def create_streaming_table(self, struct_schema, target_path=None):
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        target_table = (
            f"{self.dataflowSpec.targetDetails['database']}.{self.dataflowSpec.targetDetails['table']}"
            if self.uc_enabled and self.dpm_enabled
            else self.dataflowSpec.targetDetails['table']
        )
        dlt.create_streaming_table(
            name=target_table,
            table_properties=self.dataflowSpec.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            path=target_path,
            schema=struct_schema,
            expect_all=expect_all_dict,
            expect_all_or_drop=expect_all_or_drop_dict,
            expect_all_or_fail=expect_all_or_fail_dict,
        )

    def get_dq_expectations(self):
        """Retrieves the data quality expectations for the table."""
        expect_all_dict = None
        expect_all_or_drop_dict = None
        expect_all_or_fail_dict = None
        if self.table_has_expectations():
            data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)
            if "expect_all" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect_all"]
            if "expect" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect"]
            if "expect_all_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
            if "expect_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
            if "expect_all_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
            if "expect_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        return expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict

    def run_dlt(self):
        """Generic Method to run DLT for Bronze/Silver/Gold flows
        """
        # pipelineconf = self.spark.sparkContext.getConf().getAll()
        # for val in pipelineconf:
        #     if val[0] == 'pipelines.id':
        #         pipeline_id = val[1]
        print("Inside the run DLT method")  
        logger.info("in run_dlt function")
        self.read()
        self.write()

    @staticmethod
    def invoke_dlt_pipeline(spark,
                            layer,
                            bronze_custom_transform_func: Callable = None,
                            silver_custom_transform_func: Callable = None,
                            gold_custom_transform_func: Callable = None,
                            next_snapshot_and_version: Callable = None):
        """Invoke dlt pipeline will launch dlt with given dataflowspec."""
        dataflowspec_list = None
        if "bronze" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", dataflowspec_list, bronze_custom_transform_func, next_snapshot_and_version
            )
        elif "silver" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", dataflowspec_list, silver_custom_transform_func, next_snapshot_and_version
            )
        elif "gold" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_gold_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "gold", dataflowspec_list, gold_custom_transform_func, next_snapshot_and_version
            )
        elif "bronze_silver" == layer.lower():
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func
            )
            silver_dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", silver_dataflowspec_list, silver_custom_transform_func
            )
        elif "bronze_gold" == layer.lower(): 
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func
            )
            gold_dataflowspec_list = DataflowSpecUtils.get_gold_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "gold", gold_dataflowspec_list, gold_custom_transform_func
            )
        elif "bronze_silver_gold" == layer.lower():  # Add full pipeline support
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func
            )
            silver_dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", silver_dataflowspec_list, silver_custom_transform_func
            )
            gold_dataflowspec_list = DataflowSpecUtils.get_gold_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "gold", gold_dataflowspec_list, gold_custom_transform_func
            )
        

    @staticmethod
    def _launch_dlt_flow(
        spark, layer, dataflowspec_list, custom_transform_func=None, next_snapshot_and_version: Callable = None
    ):
        for dataflowSpec in dataflowspec_list:
            logger.info("Printing Dataflow Spec")
            logger.info(dataflowSpec)
            quarantine_input_view_name = None
            if isinstance(dataflowSpec, BronzeDataflowSpec) and dataflowSpec.quarantineTargetDetails is not None \
                    and dataflowSpec.quarantineTargetDetails != {}:
                quarantine_input_view_name = (
                    f"{dataflowSpec.quarantineTargetDetails['table']}"
                    f"_{layer}_quarantine_inputView"
                )
            else:
                logger.info("quarantine_input_view_name set to None")
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                f"{dataflowSpec.targetDetails['table']}_{layer}_inputView",
                quarantine_input_view_name,
                custom_transform_func,
                next_snapshot_and_version
            )
            dlt_data_flow.run_dlt()