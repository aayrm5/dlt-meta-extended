import copy
import json
import logging

from delta.tables import DeltaTable
from operator import length_hint
from pyspark.sql.functions import expr, sha2, concat

from src.dataflow_spec import DataflowSpecUtils
from src.metastore_ops import DataFlowUtils, DeltaPipelinesMetaStoreOps, DeltaPipelinesInternalTableOps
from src.pii_utility_bronze import PIIUpdateUtilityBronze
from src.pii_utility_gold import PIIUpdateUtilityGold
from src.pii_utility_silver import PIIUpdateUtilitySilver


logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)

class PIIUpdateUtility:
    def __init__(self, spark, dict_obj):

        self.spark = spark
        self.dict_obj = dict_obj
        self.config_dict_obj = copy.deepcopy(dict_obj)
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.DataFlowUtils = DataFlowUtils(self.spark)
        

        self.framework_columns = ["hash_key","hash_value","record_indicator","job_identifier","BatchRunDate","record_insertion_date_fw","record_update_date_fw","start_date","end_date"]

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """
        This function validates if a dictionary object has all the required attributes.
        Args:
            attributes ([List]): A list of strings representing the expected keys in the dictionary object
            dict_obj ([Dict]): A dictionary object that needs to be validated against a list of attributes
        """
        
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(set(attributes), attributes_keys)
            logger.info("missing attributes : {}".format(set(attributes).difference(attributes_keys)))
            raise ValueError("missing attributes : {}".format(set(attributes).difference(attributes_keys)))
        
    def update_pii_spec(self):
        """This method will onboard dataFlowSpecs for bronze, silver and gold

        Args:
            spark (SparkSession): [SparkSession]
            dict_obj ([type]): Required attributes are given below list
                                attributes = [
                                "pii_update_file_path",
                                "database",
                                "env",
                                "bronze_dataflowspec_path",
                                "silver_dataflowspec_path",
                                "gold_dataflowspec_path",
                                ]
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "bronze_dataflowspec_table",
            "silver_dataflowspec_table",
            # "gold_dataflowspec_table",
            "env"
        ]
        self.__validate_dict_attributes(attributes, self.dict_obj)
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        minVersion = self.spark.conf.get("spark.dltMeta.min.version", "2.0.0")
        maxVersion = self.spark.conf.get("spark.dltMeta.max.version", "")
        PIIUpdateUtilityBronze(self.spark, self.dict_obj, self.__validate_dict_attributes,self.__get_pii_update_file_dataframe,self.__get_current_dataflow_spec,self.__update_pii_cdc).update_pii_details_bronze()
        PIIUpdateUtilitySilver(self.spark, self.dict_obj, self.__validate_dict_attributes,self.__get_pii_update_file_dataframe,self.__get_current_dataflow_spec,self.__update_pii_cdc).update_pii_details_silver()
        # PIIUpdateUtilityGold(self.spark, self.dict_obj, self.__validate_dict_attributes,self.__get_pii_update_file_dataframe,self.__get_current_dataflow_spec,self.__update_pii_cdc).update_pii_details_gold()  
        
    def __get_pii_update_file_dataframe(self, pii_update_file_path):
        """
        This function reads a JSON file and returns a dataframe, raising an exception if the file format is
        not supported.
        Args:
        pii_update_file_path([Path]): This is a string parameter that represents the file path of the PII details 
                                      file that needs to be read
        Returns:
            A DataFrame object containing the data from the onboarding file specified in the 
            input parameter 'pii_update_file_path'.
        """
        pii_df = None
        if pii_update_file_path.lower().endswith(".json"):
            pii_df = self.spark.read.option("multiline", "true").json(pii_update_file_path)
            self.pii_file_type = "json"
        else:
            raise Exception("PII file format not supported! Please provide json file format")
        return pii_df

    def __decryption_data(self,data,piiFields):
        
        piiFieldsList = list(map(lambda x: x.lower(), piiFields.keys() if piiFields else {}))
        datasetColumns = list(map(lambda x: x.lower(),data.columns))
        missingColumns = list(set(piiFieldsList).difference(datasetColumns))
        if(length_hint(piiFieldsList) == 0) :
            return data
        ##if(length_hint(missingColumns) > 0) :
           ## raise Exception(f"PII fields listed {missingColumns} are not present in source dataset columns {data.columns}")
        for column in piiFields:
            datatype = piiFields[column]
            if(datatype is not None) :
                data = data.withColumn(column,expr(f"decryptDatabricks({column})"))
                if(datatype.lower().count("array") or  datatype.lower().count("struct")) :
                    data = data.withColumn(column, expr(f"from_json({column}, '{datatype}')"))
                else :
                    data = data.withColumn(column, expr(f"cast({column} as {datatype})"))
        return data
    
    def __encryption_data(self,data,piiFields):
        piiFieldsList = list(map(lambda x: x.lower(), piiFields.keys()))
        datasetColumns = list(map(lambda x: x.lower(),data.columns))
        missingColumns = list(set(piiFieldsList).difference(datasetColumns))
        if(len(piiFieldsList) == 0) :
            return data
        if(length_hint(missingColumns) > 0) :
            raise Exception(f"PII fields listed {missingColumns} are not present in source dataset columns {data.columns}")
        for column in piiFields:
            datatype = piiFields[column]
            if(datatype is not None) :
                if(datatype.lower().count("array") or  datatype.lower().count("struct")) :
                    data = data.withColumn(column, expr(f"to_json({column})"))
                else :
                    data = data.withColumn(column, expr(f"cast({column} as string)"))
                data = data.withColumn(column ,expr(f"encryptDatabricks({column})"))
        return data

    def __get_current_dataflow_spec(self,database,table,dataFlowId,dataFlowGroup):
        existing_row = self.spark.sql(f"select * from {database}.{table} where dataFlowId = '{dataFlowId}' and dataFlowGroup = '{dataFlowGroup}'")
        if(existing_row.count() == 1):
            return existing_row.collect()[0]
        elif(existing_row.count() == 0):
            raise Exception("No existing data flow specification found for dataFlowId {} and dataFlowGroup {}. PII Update is not possible".format(dataFlowId,dataFlowGroup))
        elif(existing_row.count() > 1):
            raise Exception("Multiple existing data flow specifications found for dataFlowId {} and dataFlowGroup {}. Please validate".format(dataFlowId,dataFlowGroup))

    def __update_pii_cdc(self,existing_row,pii_update_row,dataFlowSpecTable):

        existing_pii_fields = existing_row["targetPiiFields"]
        new_pii_fields = pii_update_row["targetPiiFields"]
        new_source_pii_fields = pii_update_row["sourcePiiFields"] if pii_update_row["sourcePiiFields"] else {}
        print("----------------updating pii cdc-----------------------")
        print("-----------existing pii fields-------"+str(existing_pii_fields))
        print("-----------new pii fields-------"+str(new_pii_fields))
        if "key_columns" in existing_row:
            existing_key_columns = existing_row["key_columns"]
        else:
            existing_key_columns = []

        colListSrc=["key","Value"]
        # PIIsourceConfigDF = self.DataFlowUtils.createDFKeyValues(existing_pii_fields,colListSrc)
        # PIItargetConfigDF = self.DataFlowUtils.createDFKeyValues(new_pii_fields,["key", "Value"])
        PIIsourceConfigDF = self.DataFlowUtils.createDFKeyValues(new_pii_fields,["keyIn", "ValueIn"])
        PIItargetConfigDF = self.DataFlowUtils.createDFKeyValues(existing_pii_fields,colListSrc)        
        #print("---piisoucedf---")
        #PIIsourceConfigDF.show()
        #print("----pii target df----")
        #PIItargetConfigDF.show()
        
        if PIIsourceConfigDF.count() == 0 and PIItargetConfigDF.count() > 0:
            differenceCheckDF = PIItargetConfigDF
        elif PIIsourceConfigDF.count() > 0 and PIItargetConfigDF.count() == 0:
            differenceCheckDF = PIIsourceConfigDF
        else:
            differenceCheckDF = self.DataFlowUtils.validatingpiifields(PIIsourceConfigDF,PIItargetConfigDF)
        print("---differencecheckdf is----")
        differenceCheckDF.show()
        if differenceCheckDF.count() > 0:
            print("--------difference check count>0")
            existing_target_details_dict = existing_row["targetDetails"]
            print("existing target details-------"+str(existing_target_details_dict))
            existing_target_table_path = existing_target_details_dict["path"]

            
            existing_cdc_details = existing_row["cdcApplyChanges"]
            existing_cdc_details_dict = json.loads(str(existing_cdc_details))

            if "except_column_list" in existing_cdc_details_dict:
                existing_exclude_cols = existing_cdc_details_dict["except_column_list"]
            else:
                existing_exclude_cols = []

            existing_table = f"{existing_target_details_dict['database']}.{existing_target_details_dict['table']}"

            if "cdc_type" in existing_cdc_details_dict and existing_cdc_details_dict.get("cdc_type")!= "":
                print("-------cdc_type in "+str(existing_cdc_details_dict))
                if existing_cdc_details_dict["cdc_type"] in ['0','1']:
                    staging_history = self.spark.sql(f"desc history delta.`{existing_target_table_path}_Staging`")
                    staging_prior_to_run_version = staging_history.head(1)[0]["version"]
                    main_history = self.spark.sql(f"desc history delta.`{existing_target_table_path}`")
                    main_prior_to_run_version = main_history.head(1)[0]["version"]
                    try:
                        self.__update_data(existing_target_table_path,existing_pii_fields,new_pii_fields,existing_key_columns,existing_table,existing_exclude_cols)
                    except Exception as e:
                        self.__restore_to_version(existing_target_table_path,main_prior_to_run_version)
                        raise Exception(f"PII Process Failed with Exception {e} and Restored to version {main_prior_to_run_version}")
                    try:
                        if "except_column_list" in existing_cdc_details_dict:
                            existing_exclude_cols = existing_cdc_details_dict["except_column_list"]
                        else:
                            existing_exclude_cols = []
                        
                        self.__update_data(f"{existing_target_table_path}_Staging",existing_pii_fields,new_pii_fields,existing_key_columns,f"{existing_table}_Staging",existing_exclude_cols)
                        self.__update_metadata(new_pii_fields,dataFlowSpecTable,existing_row["dataFlowId"],existing_row["dataFlowGroup"],new_source_pii_fields)
                    except Exception as e:
                        self.__restore_to_version(f"{existing_target_table_path}_Staging",staging_prior_to_run_version)
                        self.__restore_to_version(existing_target_table_path,main_prior_to_run_version)
                        raise Exception(f"PII Process Failed with Exception {e} and Restored staging and main tables to prior version ")
            else:
                main_history = self.spark.sql(f"desc history delta.`{existing_target_table_path}`")
                main_prior_to_run_version = main_history.head(1)[0]["version"]
                try:
                    self.__update_data(existing_target_table_path,existing_pii_fields,new_pii_fields,existing_key_columns,existing_table,existing_exclude_cols)
                    self.__update_metadata(new_pii_fields,dataFlowSpecTable,existing_row["dataFlowId"],existing_row["dataFlowGroup"],new_source_pii_fields)
                except Exception as e:
                    self.__restore_to_version(existing_target_table_path,main_prior_to_run_version)
                    raise Exception(f"PII Process Failed with Exception {e} and Restored to version {main_prior_to_run_version}")
                
                
    def __update_data(self,existing_target_table_path,existing_pii_fields,new_pii_fields,existing_key_columns,existing_table,exlusion_cols):

        
        existing_data = DeltaTable.forPath(self.spark,existing_target_table_path).toDF()

        decryptedData = self.__decryption_data(existing_data,existing_pii_fields)

        encryptedData = self.__encryption_data(decryptedData,new_pii_fields)

        dataframe_colslist=encryptedData.columns

        exclude_cols = []

        exclude_cols = exlusion_cols
        
        exclude_cols_list = list(set(dataframe_colslist).intersection(set(self.framework_columns)))

        exclude_cols.extend(exclude_cols_list)

        key_columns = [col for col in existing_key_columns if col not in exclude_cols] if existing_key_columns else [col for col in encryptedData.columns if (col not in exclude_cols) and (not(str(col).startswith("__")))]

        exclude_cols_value = exclude_cols
        exclude_cols_value.extend(key_columns)

        val_cols = [col for col in encryptedData.columns if (col not in exclude_cols_value) and (not(str(col).startswith("__")))]

        if not val_cols:
            val_cols = key_columns
        #sorting key and value columns to avoid issues in hash_key and hash_value generation
        key_columns.sort()
        val_cols.sort()
        keyColumns = [] 
        valueColumns = [] 
        #adding expressions for keyColumn generation for nvl case
        for key in key_columns:
            keyColumns.append(expr(f"nvl(cast(`{key}` as string),'null')"))
        #adding expressions for valueColumns generation for nvl case
        for val in val_cols: 
            valueColumns.append(expr(f"nvl(cast(`{val}` as string),'null')"))
        #generating hash_key and hash_value as sha2(concat(*valueColumns/*keyColumns),256)

        hashkey_generated_batch_df = encryptedData.withColumn("hash_value",sha2(concat(*valueColumns),256)).withColumn("hash_key",sha2(concat(*keyColumns),256))

        self.spark.sql(f"drop table if exists {existing_table}_pii_update_tmp")
        
        hashkey_generated_batch_df.write.format("delta").saveAsTable(f"{existing_table}_pii_update_tmp")

        self.spark.table(f"{existing_table}_pii_update_tmp").display()

        existing_table_details = DeltaTable.forPath(self.spark,existing_target_table_path).detail()
        existing_partitions_list = existing_table_details.select("partitionColumns").collect()[0][0]
        existing_partition_string = (",".join(existing_partitions_list)).replace("`","") if existing_partitions_list else None

        hashkey_generated_batch_df.display()

        self.spark.sql(f"delete from delta.`{existing_target_table_path}` where 1=1")
        if existing_partition_string:
            self.spark.sql(f"insert overwrite delta.`{existing_target_table_path}` partition ({existing_partition_string}) select * from {existing_table}_pii_update_tmp where 1=1")
        else:
            self.spark.sql(f"insert overwrite delta.`{existing_target_table_path}` select * from {existing_table}_pii_update_tmp where 1=1")
        self.spark.sql(f"drop table if exists {existing_table}_pii_update_tmp")

    def __update_metadata(self,new_pii_fields,dataFlowSpecTable,dataFlowId,dataFlowGroup,new_source_pii_fields):
        keys = new_pii_fields.keys()
        
        map_elements = []
        for key in keys:
            map_elements.append(key)
            map_elements.append(new_pii_fields[key])

        pii_string  = json.dumps(map_elements).replace("[",'').replace(']','')
        
        self.spark.sql(f"update {dataFlowSpecTable} set targetPiiFields = map({pii_string}) where dataFlowId = '{dataFlowId}' and dataFlowGroup = '{dataFlowGroup}'")

        if new_source_pii_fields:
            source_keys = new_source_pii_fields.keys()
            source_map_elements = []
            for key in source_keys:
                source_map_elements.append(key)
                source_map_elements.append(new_source_pii_fields[key])
            
            source_pii_string  = json.dumps(source_map_elements).replace("[",'').replace(']','')
            self.spark.sql(f"update {dataFlowSpecTable} set sourcePiiFields = map({source_pii_string}) where dataFlowId = '{dataFlowId}' and dataFlowGroup = '{dataFlowGroup}'")
        
    def __restore_to_version(self,existing_target_table_path,verion_number):
        self.spark.sql(f"RESTORE TABLE delta.`{existing_target_table_path}` TO VERSION AS OF {verion_number}")