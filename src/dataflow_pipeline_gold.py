import logging
import dlt

from src.dataflow_spec import  DataflowSpecUtils, GoldDataflowSpec
from pyspark.sql.types import *

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)


class GoldDataflowPipeline:
    """This class uses dataflowSpec of Silver/Gold capturing processing logic to launch DLT for Silver/Gold flows
    """
    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine, encryptDataset, decryptDataset):
        """Constructor Method to initialise DataFlowPipeline for Gold Pipe
        Args:
            spark ([SparkSession]): initialised Spark Session Object from pipeline
            dataflow_spec ([GoldDataflowSpec]): DataFlowSpec against which pipeline run has to be done
            view_name ([String]): DLT view name to be references during the runs
            job_arguments ([Json]): job arguments valid for the run
            view_name_quarantine ([String]): quarantine view name valid for Data Quality quarantined data , reserved for future releases
            encryptDataset([function]): Generic util method to trigger encrypt of target dataframe
            encryptDataset([function]): Generic util method to trigger decrypt of source dataframe
            write_generic([function]): Generic util method to trigger trigger write of target dataframe
            fetchJobArguments([function]): Generic util method to fetch job arguments for the run

        Raises:
            ValueError: Generic Error for the pipeline run """
        self.spark = spark
        self.refreshStreamingView = "true"
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        # self.job_arguments = job_arguments
        self.encryptDataset = encryptDataset
        self.decryptDataset = decryptDataset
        # self.write_generic = write_generic
        # self.fetchJobArguments = fetchJobArguments
        # self.apply_data_standardisation = apply_data_standardisation
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        self.table_name = f"{gold_dataflow_spec.targetDetails['table']}"
        # self.table_path = gold_dataflow_spec.targetDetails["path"]
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        cdc_apply_changes = getattr(dataflow_spec, 'cdcApplyChanges', None)
        if cdc_apply_changes:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        if type(dataflow_spec) == GoldDataflowSpec:
            self.gold_schema = None

    def read(self):
        """Generic Method to read source data for Silver/Gold pipeline run
        Raises:
            ValueError: Generic Error for the pipeline run """
        logger.info("In read function")
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        if gold_dataflow_spec.isStreaming == "true":
            self.read_gold()
            return self.get_gold_dlt_views()
        elif gold_dataflow_spec.isStreaming == "false":
            self.read_gold_batch()
            return self.get_gold_dlt_views()

    def read_gold(self):
        """Generic Method to read sources configured in transformation part of Silver/Gold layer for streaming use cases - Reserved for future
           - all the sources are registered as dlt views by their reference_name in source configuration
        Raises:
            ValueError: Generic Error for the pipeline run """
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec

        for dlt_view in gold_dataflow_spec.sources:
            print("----------------type of dlt_view--------------"+str(type(dlt_view)))
            if "source_catalog" in dlt_view:
                catalog=dlt_view["source_catalog"]
            else:
                catalog=None
            gold_util = GoldSourceProcessingUtils(self.spark,gold_dataflow_spec, dlt_view["reference_name"], self.table_name, dlt_view["source_table"], dlt_view["filter_condition"], dlt_view["pii_fields"], self.decryptDataset, dlt_view["is_streaming"] if "is_streaming" in dlt_view else "false",catalog)
            gold_util.register_source()

    def read_gold_batch(self):
        """Generic Method to read sources configured in transformation part of Silver/Gold layer for batch use cases
           - all the sources are registered as dlt views by their reference_name in source configuration
        Raises:
            ValueError: Generic Error for the pipeline run """
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        for dlt_view in gold_dataflow_spec.sources:
            print("----------------type of dlt_view--------------"+str(type(dlt_view)))
            if "source_catalog" in dlt_view:
                catalog=dlt_view["source_catalog"]
            else:
                catalog=None
            gold_util = GoldSourceProcessingUtils(self.spark,gold_dataflow_spec, dlt_view["reference_name"], self.table_name, dlt_view["source_table"], dlt_view["filter_condition"], dlt_view["pii_fields"], self.decryptDataset, "false",catalog)
            gold_util.register_source()
    def get_gold_dlt_views(self):
        """Generic Method to read and process sql transformation configured in transformation part of Silver/Gold layer.
           - all the transfomation sqls are registered as dlt views by their reference_name in configuration
           - Last sql transformatin in the array is the view i.e. persisted in as target silver/gold dataframe into target delta sink.
        Raises:
            ValueError: Generic Error for the pipeline run """
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        # data_standard_fields = gold_dataflow_spec.dataStandardFields
        tableName = f"{gold_dataflow_spec.targetDetails['table']}"
        viewName = "Silver_Gold_Views_Not_Defined"
        for index , dlt_view in enumerate(gold_dataflow_spec.dlt_views):
            viewName = dlt_view["reference_name"]
            encrypt_data = "false"
            if( index == len(gold_dataflow_spec.dlt_views) -1 ) :
                encrypt_data = "true"
            view_processing = GoldDltViewUtils(self.spark, self.table_name, viewName, dlt_view["sql_condition"], gold_dataflow_spec.targetPiiFields, encrypt_data, self.encryptDataset, gold_dataflow_spec) # Option gold_dataflow_spec added as parm in encryptDataset : Added By Aravind Ravi (Erwin Integration)
            view_processing.register_dlt_view()
        return viewName

    def cdc_apply_changes(self):
        """Legacy code to write bronze tables with dlt based cdc - reference for future for streaming CDC cases
           will be restructed and removed in future """
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        struct_schema = (
            StructType.fromJson(self.schema_json)
            if type(self.dataflowSpec) == GoldDataflowSpec
            else self.gold_schema
        )

        if cdc_apply_changes.except_column_list:
            modified_schema = StructType([])
            for field in struct_schema.fields:
                if field.name not in cdc_apply_changes.except_column_list:
                    modified_schema.add(field)
            struct_schema = modified_schema

        if cdc_apply_changes.cdc_type == "2":
            for field in struct_schema.fields:
                if field.name == cdc_apply_changes.sequence_by:
                    struct_schema.add(StructField("__START_AT", field.dataType))
                    struct_schema.add(StructField("__END_AT", field.dataType))
                    break

        dlt.create_streaming_live_table(
            name=f"{self.dataflowSpec.targetDetails['table']}",
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            path=self.dataflowSpec.targetDetails["path"],
            schema=struct_schema,
        )

        dlt.apply_changes(
            target=f"{self.dataflowSpec.targetDetails['table']}",
            source=self.view_name,
            keys=cdc_apply_changes.key_columns,
            sequence_by=cdc_apply_changes.sequence_by,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=cdc_apply_changes.apply_as_deletes,
            apply_as_truncates=cdc_apply_changes.apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=None,
            stored_as_scd_type=cdc_apply_changes.cdc_type,
        )


    #adding for gold
    def write_gold_view(self, view_name):
        """Generic Method to trigger write logic Silver/Gold layer.
          write_generic is triggered with required arguments to save the data.
        Args:
            view_name ([String]): source DLT view name to be written
        Raises:
            ValueError: Generic Error for the pipeline run """
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        view_name = view_name
        database_name = f"{gold_dataflow_spec.targetDetails['database']}"
        table_name = f"{gold_dataflow_spec.targetDetails['table']}"
        # table_path = gold_dataflow_spec.targetDetails["path"]
        isPartitionedByRunDate = gold_dataflow_spec.isPartitionedByRunDate
        partitionColumns = gold_dataflow_spec.partitionColumns
        writerConfigOptions = gold_dataflow_spec.writerConfigOptions
        cdcApplyChanges =gold_dataflow_spec.cdcApplyChanges
        # jobArguments = self.fetchJobArguments()
        # jobIdentifier = jobArguments[0]
        # batchRunDate = jobArguments[1]
        if (self.dataflowSpec.isStreaming == "true") & (self.cdc_apply_changes is not None):
            self.cdc_apply_changes()
        else:
            self.write_gold()

class GoldSourceProcessingUtils:
    """This class handles all utility methods to manage and read individual sources in Silver/Gold pipeline run"""
    def __init__(self, spark, gold_dataflow_spec, view_name,table_name, source_table, filter_condition, pii_fields, decrypt_function, streaming,source_catalog):
        print("-----------------goldsourceprocessingutils-----------------")
        """Constructor Method to initialise GoldSourceProcessingUtils
        Args:
            spark ([SparkSession]): initialised Spark Session Object from pipeline
            view_name ([String]): DLT view name to be created for required source of Silver/Gold flow.
            source_table ([String]): <database.table> source bronze/silver table from which data has to be read ,if source is part of single DLT run and dependency has to be maanged in DLT run ,db name should be LIVE
            filter_condition ([String]): sql filter condition to filter required data based on CDC type ,details can be referred here : https://dev.azure.com/petronasvsts/EDH%20Core%20Team/_wiki/wikis/EDH-Core-Team.wiki/18211/Metadata-Preparation?anchor=3.-gold/silver-transformation-file-structure
            pii_fields([Dictionary<String,String]): pii fields details to decrypt source data for pipeline run
            decrypt_function([function]): Decrypt function to decrypt source data
            streaming([string]): true/false to read source data as stream or batch

        Raises:
            ValueError: Generic Error for the pipeline run """
        self.spark = spark
        self.dataflowSpec = gold_dataflow_spec
        self.view_name= view_name
        self.table_name=table_name
        # self.table_path=table_path
        self.source_table = source_table
        self.filter_condition = filter_condition
        self.pii_fields = pii_fields
        self.decrypt_function = decrypt_function
        self.streaming = streaming
        self.source_catalog = source_catalog
    def register_source(self) :
        """Generic Method to regsiter source table as dlt view"""
        @dlt.view(
            name= self.view_name,
            comment=f"""input dataset view for {self.view_name}""",
        )
        def read_dlt_gold():
            # print("----read-dlt-gold-----")
            # print("------------------------tablepath is----------------------"+str(self.table_path))
            # enableCache=self.dataflowSpec.enableCache
            # enableCheckpoint=self.dataflowSpec.enableCheckpoint
            # print("---------------------enablecache is-----------------"+str(enableCache))
            # print("---------------------enable checkpoint is-----------------"+str(enableCheckpoint))            
            #raw_delta_table_stream = self.decryptDataset(data = ( self.spark.sql(f""" select * from {dlt_view["source_table"]}""")), piiFields=dlt_view["pii_fields"])
            if(self.streaming == "true") :
                raw_delta_table_stream = self.spark.sql(f"select * from STREAM({self.source_table})")
            else :
                raw_delta_table_stream = self.spark.sql(f"select * from {self.source_table}")
            raw_delta_table_stream = self.decrypt_function(raw_delta_table_stream, self.pii_fields)
            if(self.filter_condition is not None and len(self.filter_condition.strip()) > 0 ) :
                raw_delta_table_stream = raw_delta_table_stream.where(self.filter_condition)
            # if(enableCache == "true" and enableCache is not None) :
            #     print("-----enable cache is true----")
            #     raw_delta_table_stream = raw_delta_table_stream.cache()
            # if(enableCheckpoint == "true" and enableCheckpoint is not None) :
            #     print("----------------enable checkpoint true 1")
            #     print("-------getvhekpointdir is--------------"+str(self.spark.sparkContext.getCheckpointDir()))
            #     if(self.spark.sparkContext.getCheckpointDir() is not None) :
            #         print("-----enable checkpoint is true and not none----")
            #         directory=self.table_path+"_tmp_cp"
            #         print("----directory----"+str(directory))
            #         self.spark.sparkContext.setCheckpointDir(directory)
            #     raw_delta_table_stream = raw_delta_table_stream.checkpoint()
            return raw_delta_table_stream
            

class GoldDltViewUtils:
    """This class handles all utility methods to manage and read individual transformations in Silver/Gold pipeline run"""
    def __init__(self, spark,table_name, view_name, sql_condition, pii_fields, encrypt_data, encrypt_function, gld_df_spec):
        print("-----------------GoldDltViewUtils-----------------")
        """Constructor Method to initialise GoldDltViewUtils
        Args:
            spark ([SparkSession]): initialised Spark Session Object from pipeline
            view_name ([String]): DLT view name to be created for required transformation of Silver/Gold flow.
            sql_condition ([String]): sql condition capturing required transformation logic
            pii_fields([Dictionary<String,String]): pii fields details to encrypt final dataframe data for pipeline run
            encrypt_data([string]):true/false - whether to encrypt the dataframe or not ,is true for final dataframe in array of sql transformations.
            encrypt_function([function]): Encrypt function to encrypt target data

        Raises:
            ValueError: Generic Error for the pipeline run """
        self.spark = spark
        self.table_name=table_name
        # self.table_path=table_path
        self.view_name= view_name
        self.sql_condition = sql_condition
        self.pii_fields = pii_fields
        self.encrypt_data = encrypt_data
        self.encrypt_function = encrypt_function
        # self.data_standardisation_function = data_standardisation_function
        # self.data_standard_fields = data_standard_fields
        self.gld_df_spec = gld_df_spec # Option gld_df_spec added as parm in encryptDataset : Added By Aravind Ravi (Erwin Integration)
    def register_dlt_view(self) :
        """Generic Method to register sql transformation and encrypt logic in Gold/Silver pipeline run"""
        @dlt.view(
                name= self.view_name,
                comment=f"""input dataset view for {self.view_name}""",
            )
        def read_dlt_gold_views():
            print("read_dlt_gold_views")
            # print("------------------------tablepath is----------------------"+str(self.table_path))
            # enableCache=self.gld_df_spec.enableCache
            # enableCheckpoint=self.gld_df_spec.enableCheckpoint
            dataframe = self.spark.sql(self.sql_condition)
            # print("---------------------enablecache is-----------------"+str(enableCache))
            # print("---------------------enable checkpoint is-----------------"+str(enableCheckpoint))
            # catalog_name=self.gld_df_spec.targetDetails.get("catalog",None)
            if(self.encrypt_data == "true") :
                #dataframe =  self.encrypt_function(dataframe, self.pii_fields)
                dataframe = self.encrypt_function(dataframe, self.pii_fields, self.gld_df_spec) # Option gld_df_spec added as parm in encryptDataset : Added By Aravind Ravi (Erwin Integration)
            # if(enableCache == "true" and enableCache is not None) :
            #     print("---enable cache is true---")
            #     dataframe = dataframe.cache()
            # if(enableCheckpoint == "true" and enableCheckpoint is not None) :
            #     print("----enable checkpoint is true---")
            #     print("-------getvhekpointdir is--------------"+str(self.spark.sparkContext.getCheckpointDir()))
            #     if(self.spark.sparkContext.getCheckpointDir() is not None):
            #         print("------------------------tablepath is----------------------"+str(self.table_path))
            #         directory=self.table_path+"_tmp_cp"
            #         print("----directory----"+str(directory))
            #         self.spark.sparkContext.setCheckpointDir(directory)
                # dataframe = dataframe.checkpoint()
            return dataframe