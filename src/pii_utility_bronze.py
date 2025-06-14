import json
import logging

from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import countDistinct, sha2, concat, expr

from delta.tables import DeltaTable

# from src.metastore_ops import DataFlowUtils

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)

class PIIUpdateUtilityBronze:
    def __init__(self,spark,dict_obj,validate_dict_attributes,get_pii_update_file_dataframe,get_current_dataflow_spec,update_pii_cdc):
        self.spark = spark
        self.dict_obj = dict_obj
        self.__validate_dict_attributes = validate_dict_attributes
        self.__get_pii_update_file_dataframe = get_pii_update_file_dataframe
        self.__get_current_dataflow_spec =  get_current_dataflow_spec
        self.__update_pii_cdc = update_pii_cdc
        # self.DataFlowUtils = DataFlowUtils(self.spark)
        # self.framework_columns = ["hash_key","hash_value","record_indicator","job_identifier","BatchRunDate","record_insertion_date_fw","record_update_date_fw","start_date","end_date"]


    def update_pii_details_bronze(self):
        attributes = [
            "onboarding_file_path",
            "database",
            "bronze_dataflowspec_table",
            "silver_dataflowspec_table",
            # "gold_dataflowspec_table",
            "env"
        ]

        dict_obj = self.dict_obj
        self.__validate_dict_attributes(attributes, dict_obj)
        pii_update_file_df = self.__get_pii_update_file_dataframe(dict_obj["onboarding_file_path"])
        pii_update_spec_df = self.__get_bronze_pii_update_spec_dataframe(pii_update_file_df)

        database = dict_obj["database"]
        table = dict_obj["bronze_dataflowspec_table"]

        pii_update_spec_rows = pii_update_spec_df.collect()

        for row in pii_update_spec_rows:
            existing_row = self.__get_current_dataflow_spec(database,table,row["dataFlowId"],row["dataFlowGroup"])
            self.__update_pii_cdc(existing_row,row,f"{database}.{table}")
            
    
    def __get_bronze_pii_update_spec_dataframe(self, pii_df):
        """
        This function generates a dataframe containing specifications for a bronze data flow based on input
        parameters.
        Args:
            onboarding_df ([DataFrame]): A DataFrame containing the onboarding information for a data flow
            env ([String]): The environment for which the data flow specification dataframe is being generated (e.g.
                            "dev", "prod", etc.)
        Returns:
            A DataFrame with the specified schema and columns, which is created by processing the input
            DataFrame `onboarding_df` and some environment variables. The DataFrame contains information about
            the data flow specification for a bronze table, including details about the source, target, schema,
            partitioning, and data quality expectations.
        """

        pii_update_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "targetPiiFields",
            "sourcePiiFields"             
            ]

        pii_update_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("targetPiiFields", MapType(StringType(), StringType(), True), True),
                StructField("sourcePiiFields", MapType(StringType(), StringType(), True), True),
            ]
        )

        data = []
        pii_rows = pii_df.collect()


        pii_row_columns = pii_df.columns
        
        id_gen = True
        flow_id = 1
        if "data_flow_id" in pii_row_columns:
            if pii_df.count() == pii_df.select(countDistinct("data_flow_id")).collect()[0][0]:
                id_gen = False

        for pii_row in pii_rows:

            if id_gen:
                data_flow_spec_id = flow_id
                flow_id = flow_id + 1
            else:
                data_flow_spec_id = pii_row["data_flow_id"]

            data_flow_spec_group = pii_row["data_flow_group"]
            target_pii_fields={}
            target_pii_fields_json = {}


            if(pii_row_columns.count("targetPiiFields") == 1) :
                target_pii_fields_json = pii_row["targetPiiFields"]
            if target_pii_fields_json:
                json_pii_fields = target_pii_fields_json.asDict()
                for piiField in json_pii_fields :
                    if(json_pii_fields[piiField]):
                        target_pii_fields[piiField] = json_pii_fields[piiField]
            
            bronze_row = (
                data_flow_spec_id,
                data_flow_spec_group,
                target_pii_fields,
                {},

            )
            data.append(bronze_row)
            # logger.info(bronze_parition_columns)

        pii_update_rows_df = self.spark.createDataFrame(data, pii_update_spec_schema).toDF(*pii_update_spec_columns)

        return pii_update_rows_df