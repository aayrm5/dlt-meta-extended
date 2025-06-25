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
        self.env = getattr(dataflow_spec, 'env', 'dev')
        self.view_name = view_name
        self.encryptDataset = encryptDataset
        self.decryptDataset = decryptDataset
        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        self.database_name = gold_dataflow_spec.targetDetails['database']
        self.table_name = gold_dataflow_spec.targetDetails['table']
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
        else:
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
            gold_util = GoldSourceProcessingUtils(self.spark,gold_dataflow_spec, dlt_view["reference_name"], dlt_view[f"source_table_{self.env}"], dlt_view["filter_condition"], dlt_view["pii_fields"], self.decryptDataset, dlt_view["is_streaming"] if "is_streaming" in dlt_view else "true")
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
            gold_util = GoldSourceProcessingUtils(self.spark, gold_dataflow_spec, dlt_view["reference_name"], dlt_view[f"source_table_{self.env}"], dlt_view["filter_condition"], dlt_view["pii_fields"], self.decryptDataset, "false")
            gold_util.register_source()

    def get_gold_dlt_views(self):
        """Generic Method to read and process sql transformation configured in transformation part of Silver/Gold layer.
           - all the transfomation sqls are registered as dlt views by their reference_name in configuration
           - Last sql transformatin in the array is the view i.e. persisted in as target silver/gold dataframe into target delta sink.
        Raises:
            ValueError: Generic Error for the pipeline run """

        gold_dataflow_spec: GoldDataflowSpec = self.dataflowSpec
        tableName = f"{gold_dataflow_spec.targetDetails['table']}"
        viewName = "Silver_Gold_Views_Not_Defined"

        for index , dlt_view in enumerate(gold_dataflow_spec.dlt_views):
            viewName = dlt_view["reference_name"]
            encrypt_data = "false"
            if( index == len(gold_dataflow_spec.dlt_views) -1 ) :
                encrypt_data = "true"
            view_processing = GoldDltViewUtils(self.spark, self.table_name, viewName, dlt_view["sql_condition"], gold_dataflow_spec.targetPiiFields, encrypt_data, self.encryptDataset, gold_dataflow_spec)
            view_processing.register_dlt_view()
        return viewName
    


class GoldSourceProcessingUtils:
    """This class handles all utility methods to manage and read individual sources in Silver/Gold pipeline run"""
    def __init__(self, spark, gold_dataflow_spec, view_name, source_table, filter_condition, pii_fields, decrypt_function, streaming):
        print("-----------------goldsourceprocessingutils-----------------")
        """Constructor Method to initialise GoldSourceProcessingUtils
        Args:
            spark ([SparkSession]): initialised Spark Session Object from pipeline
            view_name ([String]): DLT view name to be created for required source of Silver/Gold flow.
            source_table ([String]): <database.table> source bronze/silver table from which data has to be read ,if source is part of single DLT run and dependency has to be maanged in DLT run ,db name should be LIVE
            filter_condition ([String]): sql filter condition to filter required data based on CDC type 
            pii_fields([Dictionary<String,String]): pii fields details to decrypt source data for pipeline run
            decrypt_function([function]): Decrypt function to decrypt source data
            streaming([string]): true/false to read source data as stream or batch

        Raises:
            ValueError: Generic Error for the pipeline run """
        self.spark = spark
        self.dataflowSpec = gold_dataflow_spec
        self.view_name= view_name
        self.source_table = source_table
        self.filter_condition = filter_condition
        self.pii_fields = pii_fields
        self.decrypt_function = decrypt_function
        self.streaming = streaming

    def register_source(self) :
        """Generic Method to regsiter source table as dlt view"""
        @dlt.view(
            name= self.view_name,
            comment=f"""input dataset view for {self.view_name}""",
        )
        def read_dlt_gold():
            print("----read-dlt-gold-----")
            print("----creating the view" + self.view_name + "----")

            if(self.streaming == "true") :
                raw_delta_table_stream = self.spark.sql(f"select * from STREAM({self.source_table})")
            else :
                raw_delta_table_stream = self.spark.sql(f"select * from {self.source_table}")

            decrypted_delta_table_stream = self.decrypt_function(raw_delta_table_stream, self.pii_fields)

            if(self.filter_condition is not None and len(self.filter_condition.strip()) > 0 ) :
                decrypted_delta_table_stream = decrypted_delta_table_stream.where(self.filter_condition)

            return decrypted_delta_table_stream
            

class GoldDltViewUtils:
    """This class handles all utility methods to manage and read individual transformations in Silver/Gold pipeline run"""
    def __init__(self, spark,table_name, view_name, sql_condition, pii_fields, encrypt_data, encrypt_function, gold_dataflow_spec):
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
        self.view_name= view_name
        self.sql_condition = sql_condition
        self.pii_fields = pii_fields
        self.encrypt_data = encrypt_data
        self.encrypt_function = encrypt_function
        self.gold_dataflow_spec = gold_dataflow_spec 

    def register_dlt_view(self) :
        """Generic Method to register sql transformation and encrypt logic in Gold/Silver pipeline run"""
        @dlt.view(
                name= self.view_name,
                comment=f"""input dataset view for {self.view_name}""",
            )
        def read_dlt_gold_views():
            print("read_dlt_gold_views")
            print("----creating the view" + self.view_name + "----")

            dataframe = self.spark.sql(self.sql_condition)

            if(self.encrypt_data == "true") :

                dataframe = self.encrypt_function(dataframe, self.pii_fields, self.gold_dataflow_spec) 
            return dataframe
        



