import json
import logging
import dlt
import traceback
from operator import length_hint
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
from pyspark.sql.functions import explode,explode_outer,col


from src.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils
from src.pipeline_readers import PipelineReaders

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)


class BronzeDataflowPipeline:
    """This class uses dataflowSpec of Bronze capturing processing logic to launch DLT for Bronze flows
    """
    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine, encryptDataset, decryptDataset):
        """Constructor Method to initialise DataFlowPipeline for Bronze Pipe
       Args:
           spark ([SparkSession]): initialised Spark Session Object from pipeline
           dataflow_spec ([GoldDataflowSpec]): DataFlowSpec against which pipeline run has to be done
           view_name ([String]): DLT view name to be references during the runs
           job_arguments ([Json]): job arguments valid for the run
           view_name_quarantine ([String]): quarantine view name valid for Data Quality quarantined data , reserved for future releases
           encryptDataset([function]): Generic util method to trigger encrypt of target dataframe
           decryptDataset([function]): Generic util method to trigger decrypt of source dataframe
           write_generic([function]): Generic util method to trigger trigger write of target dataframe
           fetchJobArguments([function]): Generic util method to fetch job arguments for the run

       Raises:
           ValueError: Generic Error for the pipeline run """
        self.spark = spark
        self.dataflowSpec = dataflow_spec
        print("--------dataflowspec is ----------"+str(dataflow_spec))
        self.view_name = view_name
        # self.job_arguments = job_arguments
        self.encryptDataset = encryptDataset
        self.decryptDataset = decryptDataset
        # self.write_generic = write_generic
        # self.fetchJobArguments = fetchJobArguments
        # self.apply_data_standardisation = apply_data_standardisation
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        if type(dataflow_spec) == BronzeDataflowSpec:
            if dataflow_spec.schema:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        

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
            dataframe = self.recurFlattenDF(dataframe, bronze_dataflow_spec.columnToExtract)
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
            dataframe = self.recurFlattenDF(dataframe, column_to_extract)
        return dataframe
            
    def recurFlattenDF(self, dfNested , arrayFieldToExtract = "", level = 0):

        # Maximum depth check
        if level > 100:  # Prevent infinite recursion
            print(f"Maximum recursion depth reached at level {level}")
            print(f"Schema: {dfNested.schema.simpleString()}")
            return dfNested
        
        # Check if DataFrame is empty
        if not dfNested.columns:
            return dfNested

        rootArrayTypeCounts = 0
        arrayFieldNames = []

        # Identify Array fields in the schema
        print("--------------The level is level----------------"+str(level))
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, ArrayType):
                arrayFieldNames.append(field.name)
                if(level == 0 ) :
                    rootArrayTypeCounts = rootArrayTypeCounts + 1
                
                # Handle nested arrays (level > 0) when no specific field is targeted
                if(level > 0 and arrayFieldToExtract == ""):
                    # print("-------df count in arrayfield extract---------"+str(dfNested.count()))            
                    dfNested = dfNested.withColumn(f"{field.name}",explode_outer(f"{field.name}"))
                    # print(f"-------The rowCount after extract is --------"+str(dfNested.count()))

        # Multiple Array Columns Scenario
        if(((rootArrayTypeCounts > 1) or (rootArrayTypeCounts == 1 and len(dfNested.columns) > 1 )) and arrayFieldToExtract == "") :
            raise ValueError(f"Detected multiple columns which cannot be flattened without providing column name to be extracted, array column names : {str(arrayFieldNames)} and dataset schema : {str(dfNested.columns)}")
        # Single Array Column Scenario
        elif (rootArrayTypeCounts == 1 and len(dfNested.columns) == 1 ):
            dfNested = dfNested.withColumn(f"{arrayFieldNames[0]}",explode(f"{arrayFieldNames[0]}")).select(f"{arrayFieldNames[0]}.*")
        # Specific Array Column Extraction
        elif (arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns and isinstance(dfNested.select(arrayFieldToExtract).schema.fields[0].dataType, ArrayType)):
            dfNested = dfNested.withColumn(arrayFieldToExtract,explode(arrayFieldToExtract))
            dfNestedlist = dfNested.columns
            dfNestedlist.remove(arrayFieldToExtract)
            dfNestedlist.append(f"{arrayFieldToExtract}.*")
            dfNested = dfNested.select(dfNestedlist)
            print("---------------in arraydata type case-------------------")
            arrayFieldToExtract = ""  # Reset for next iteration
        # Specific Struct Column Extraction
        elif(arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns and isinstance(dfNested.select(arrayFieldToExtract).schema.fields[0].dataType, StructType)):
            dfNestedlist = dfNested.columns
            dfNestedlist.remove(arrayFieldToExtract)
            dfNestedlist.append(f"{arrayFieldToExtract}.*")
            dfNested = dfNested.select(dfNestedlist)
            arrayFieldToExtract = ""  # Reset for next iteration
        # Handle other specified fields
        elif(arrayFieldToExtract != "" and arrayFieldToExtract in dfNested.columns ):
            dfNestedlist = dfNested.columns           
            dfNested = dfNested.select(dfNestedlist)                     
            arrayFieldToExtract = ""  # Reset for next iteration
        # Flatten all StructType Fields
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, StructType):
                for nested_col in dfNested.select(f"{field.name}.*").columns:
                    print(f"Flattening struct field: {field.name}.{nested_col}")
                    # Create new column with flattened name
                    dfNested = dfNested.withColumn(f"{field.name}_{nested_col}", col(f"{field.name}.{nested_col}"))
                # Drop the original struct column
                dfNested = dfNested.drop(field.name)

        # print(f"DataFrame count after struct flattening: {dfNested.count()}")

        # Check for remaining nested columns
        nested_cols = []
        for field in dfNested.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                nested_cols.append(field)
        
        # Step 10: Recursive call or return
        if len(nested_cols) == 0:
            return dfNested  # Base case: no more nested structures
        elif len(nested_cols) > 0:
        # Check if we're actually making progress
            # if level > 0:
            #     current_nested = [f.name for f in dfNested.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
            #     if len(current_nested) >= len(nested_cols):
            #         print(f"No progress at level {level}, stopping recursion")
            #         return dfNested
            
            return self.recurFlattenDF(dfNested, arrayFieldToExtract, level + 1)
        else:
            return dfNested
        



















