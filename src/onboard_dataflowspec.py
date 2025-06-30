"""OnboardDataflowSpec class provides bronze/silver/gold onboarding features with flexible layer selection."""

import copy
import dataclasses
import json
import logging

import pyspark.sql.types as T
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from src.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils, SilverDataflowSpec, GoldDataflowSpec
from src.metastore_ops import DeltaPipelinesInternalTableOps, DeltaPipelinesMetaStoreOps

logger = logging.getLogger("databricks.labs.dltmeta")
logger.setLevel(logging.INFO)


class OnboardDataflowspec:
    """OnboardDataflowSpec class provides bronze/silver/gold onboarding features with flexible layer selection."""

    def __init__(self, spark, dict_obj, bronze_schema_mapper=None, uc_enabled=False):
        """Onboard Dataflowspec Constructor."""
        self.spark = spark
        self.dict_obj = dict_obj
        self.bronze_dict_obj = copy.deepcopy(dict_obj)
        self.silver_dict_obj = copy.deepcopy(dict_obj)
        self.gold_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        
        # Determine which layers to onboard based on available specifications
        self.layers_to_onboard = self._determine_layers_to_onboard()
        logger.info(f"Layers to onboard: {self.layers_to_onboard}")
        
        self.__initialize_paths(uc_enabled)
        self.bronze_schema_mapper = bronze_schema_mapper
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.onboard_file_type = None
        env = self.dict_obj.get("env", "dev")

    def _determine_layers_to_onboard(self):
        """
        Determine which layers (bronze, silver, gold) should be onboarded based on available specifications.
        
        Returns:
            list: List of layers to onboard ('bronze', 'silver', 'gold')
        """
        layers = []
        
        # Check for bronze layer
        if any(key in self.dict_obj for key in ['bronze_dataflowspec_table', 'bronze_dataflowspec_path']):
            layers.append('bronze')
        
        # Check for silver layer
        if any(key in self.dict_obj for key in ['silver_dataflowspec_table', 'silver_dataflowspec_path']):
            layers.append('silver')
            
        # Check for gold layer
        if any(key in self.dict_obj for key in ['gold_dataflowspec_table', 'gold_dataflowspec_path']):
            layers.append('gold')
        
        # If no specific layers detected, check for any data in onboarding file
        if not layers and 'onboarding_file_path' in self.dict_obj:
            try:
                # Try to read the onboarding file to determine available layers
                onboarding_df = self.__get_onboarding_file_dataframe(self.dict_obj['onboarding_file_path'], self.dict_obj['env'])
                sample_row = onboarding_df.first()
                
                # Check for bronze indicators with environment substitution
                env = self.dict_obj.get("env", "dev")
                if any(col in onboarding_df.columns for col in [f'bronze_{env}', f'source_details_{env}', 'bronze_reader_options']):
                    layers.append('bronze')
                
                # Check for silver indicators with environment substitution
                if any(col in onboarding_df.columns for col in [f'silver_{env}']):
                    layers.append('silver')
                    
                # Check for gold indicators with environment substitution
                if any(col in onboarding_df.columns for col in [f'gold_{env}']):
                    layers.append('gold')
            except Exception as e:
                logger.warning(f"Could not auto-determine layers from onboarding file: {e}")

        print("----------Riyaz------------" + str(layers))
        return layers

    def __initialize_paths(self, uc_enabled):
        """Initialize paths by removing irrelevant path configurations based on layers to onboard."""
        
        # For bronze dict, remove silver and gold specifications
        if 'bronze' in self.layers_to_onboard:
            for key in ['silver_dataflowspec_table', 'silver_dataflowspec_path', 
                       'gold_dataflowspec_table', 'gold_dataflowspec_path']:
                if key in self.bronze_dict_obj:
                    del self.bronze_dict_obj[key]
        
        # For silver dict, remove bronze and gold specifications  
        if 'silver' in self.layers_to_onboard:
            for key in ['bronze_dataflowspec_table', 'bronze_dataflowspec_path',
                       'gold_dataflowspec_table', 'gold_dataflowspec_path']:
                if key in self.silver_dict_obj:
                    del self.silver_dict_obj[key]
        
        # For gold dict, remove bronze and silver specifications
        if 'gold' in self.layers_to_onboard:
            for key in ['bronze_dataflowspec_table', 'bronze_dataflowspec_path',
                       'silver_dataflowspec_table', 'silver_dataflowspec_path']:
                if key in self.gold_dict_obj:
                    del self.gold_dict_obj[key]

        # Handle UC-specific path removal
        if uc_enabled:
            print("uc_enabled:", uc_enabled)
            if 'bronze' in self.layers_to_onboard and "bronze_dataflowspec_path" in self.bronze_dict_obj:
                del self.bronze_dict_obj["bronze_dataflowspec_path"]
            if 'silver' in self.layers_to_onboard and "silver_dataflowspec_path" in self.silver_dict_obj:
                del self.silver_dict_obj["silver_dataflowspec_path"]
            if 'gold' in self.layers_to_onboard and "gold_dataflowspec_path" in self.gold_dict_obj:
                del self.gold_dict_obj["gold_dataflowspec_path"]

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes ([type]): [description]
            dict_obj ([type]): [description]

        Raises:
            ValueError: [description]
        """
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            logger.info(
                "missing attributes : {}".format(
                    set(attributes).difference(attributes_keys)
                )
            )
            raise ValueError(
                f"missing attributes : {set(attributes).difference(attributes_keys)}"
            )

    def _validate_layer_specific_attributes(self, layer):
        """
        Validate attributes specific to a layer.
        
        Args:
            layer (str): Layer name ('bronze', 'silver', 'gold')
        """
        base_attributes = [
            "database",
            "onboarding_file_path", 
            "overwrite",
            "env",
            "version",
            "import_author" 
        ]
        
        # Add layer-specific attributes
        if layer == 'bronze':
            base_attributes.append("bronze_dataflowspec_table")
            if not self.uc_enabled:
                base_attributes.append("bronze_dataflowspec_path")
        elif layer == 'silver':
            base_attributes.append("silver_dataflowspec_table") 
            if not self.uc_enabled:
                base_attributes.append("silver_dataflowspec_path")
        elif layer == 'gold':
            base_attributes.append("gold_dataflowspec_table")
            if not self.uc_enabled:
                base_attributes.append("gold_dataflowspec_path")
        
        # Validate against the appropriate dict object
        if layer == 'bronze':
            self.__validate_dict_attributes(base_attributes, self.bronze_dict_obj)
        elif layer == 'silver':
            self.__validate_dict_attributes(base_attributes, self.silver_dict_obj)  
        elif layer == 'gold':
            self.__validate_dict_attributes(base_attributes, self.gold_dict_obj)

    def _has_layer_data(self, onboarding_row, layer, env):
        """
        Check if a specific row has data for the specified layer.
        
        Args:
            onboarding_row: The row from onboarding dataframe
            layer (str): Layer name ('bronze', 'silver', 'gold')
            env (str): Environment name
            
        Returns:
            bool: True if the row has data for the layer
        """
        try:
            layer_key = f"{layer}_{env}"
            return layer_key in onboarding_row and onboarding_row[layer_key] is not None
        except Exception as e:
            logger.warning(f"Error checking layer data for {layer}: {e}")
            return False

    def onboard_dataflow_specs(self):
        """
        Onboard_dataflow_specs method will onboard dataFlowSpecs for the determined layers.

        This method dynamically onboards only the layers that have been specified in the configuration.
        It validates each layer's requirements independently and only processes the relevant layers.
        """
        
        # Validate attributes for each layer that will be onboarded
        for layer in self.layers_to_onboard:
            try:
                self._validate_layer_specific_attributes(layer)
                logger.info(f"Validation successful for {layer} layer")
            except ValueError as e:
                logger.error(f"Validation failed for {layer} layer: {e}")
                raise

        # Onboard each layer based on the determined layers
        if 'bronze' in self.layers_to_onboard:
            logger.info("Starting bronze dataflow spec onboarding...")
            self.onboard_bronze_dataflow_spec()
            
        if 'silver' in self.layers_to_onboard:
            logger.info("Starting silver dataflow spec onboarding...")
            self.onboard_silver_dataflow_spec()
            
        if 'gold' in self.layers_to_onboard:
            logger.info("Starting gold dataflow spec onboarding...")
            self.onboard_gold_dataflow_spec()

    def _validate_onboarding_data_for_layer(self, onboarding_df, layer, env):
        """
        Validate that the onboarding dataframe contains required data for the specified layer.
        
        Args:
            onboarding_df: The onboarding dataframe
            layer (str): Layer name ('bronze', 'silver', 'gold')
            env (str): Environment name
            
        Returns:
            bool: True if data is available for the layer
        """
        try:
            # Get DataFrame columns
            df_columns = onboarding_df.columns
            
            print(f"DEBUG: Validating {layer} layer for environment: {env}")
            print(f"DEBUG: Available columns: {df_columns}")
            
            # Check for required fields based on layer
            if layer == 'bronze':
                required_fields = ['data_flow_id', 'data_flow_group', f'source_details_{env}', f'bronze_{env}']
            elif layer == 'silver':
                required_fields = ['data_flow_id', 'data_flow_group', f'silver_{env}']
            elif layer == 'gold':
                required_fields = ['data_flow_id', 'data_flow_group', f'gold_{env}']
            else:
                return False
            
            print(f"DEBUG: Required fields: {required_fields}")
            
            # Check if required fields exist in DataFrame columns
            missing_fields = [field for field in required_fields if field not in df_columns]
            
            if missing_fields:
                print(f"DEBUG: Missing fields: {missing_fields}")
                return False
            
            print(f"DEBUG: All required fields found for {layer} layer")
            return True
            
        except Exception as e:
            print(f"DEBUG: Error in validation: {e}")
            return False

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

    def register_gold_dataflow_spec_tables(self):
        """Register gold dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["gold_dataflowspec_table"],
            self.dict_obj["gold_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded gold table={self.dict_obj["database"]}.{self.dict_obj["gold_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["gold_dataflowspec_table"]}"""
        ).show()

    def onboard_bronze_dataflow_spec(self):
        """
        Onboard bronze dataflow spec.
        Modified to only process rows that have bronze layer data.
        """
        if 'bronze' not in self.layers_to_onboard:
            logger.info("Bronze layer not selected for onboarding, skipping...")
            return
            
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
            dict_obj["onboarding_file_path"], self.dict_obj['env']
        )

        # Filter rows to only include those with bronze data
        bronze_rows = []
        for row in onboarding_df.collect():
            if self._has_layer_data(row, 'bronze', dict_obj["env"]):
                bronze_rows.append(row)
        
        if not bronze_rows:
            logger.warning("No rows found with bronze layer data")
            return

        logger.info(f"Processing {len(bronze_rows)} rows for bronze layer")

        # Create DataFrame from filtered rows
        bronze_onboarding_df = self.spark.createDataFrame(bronze_rows, onboarding_df.schema)

        bronze_dataflow_spec_df = self.__get_bronze_dataflow_spec_dataframe(
            bronze_onboarding_df, dict_obj["env"]
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

    def onboard_silver_dataflow_spec(self):
        """
        Onboard silver dataflow spec.
        Modified to only process rows that have silver layer data.
        """
        if 'silver' not in self.layers_to_onboard:
            logger.info("Silver layer not selected for onboarding, skipping...")
            print("-----------------Exiting onboard silver dataflow Spec as its not present in layers to onboard list---------------------------------")
            return
            
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
            dict_obj["onboarding_file_path"], self.dict_obj['env']
        )
        
        # Filter rows to only include those with silver data
        silver_rows = []
        for row in onboarding_df.collect():
            if self._has_layer_data(row, 'silver', dict_obj["env"]):
                silver_rows.append(row)
        
        if not silver_rows:
            logger.warning("No rows found with silver layer data, skipping silver onboarding")
            print("-----------No silver layer data found in onboarding file---------------")
            return

        logger.info(f"Processing {len(silver_rows)} rows for silver layer")

        # Create DataFrame from filtered rows
        silver_onboarding_df = self.spark.createDataFrame(silver_rows, onboarding_df.schema)
            
        silver_data_flow_spec_df = self.__get_silver_dataflow_spec_dataframe(
            silver_onboarding_df, dict_obj["env"]
        )
        if silver_data_flow_spec_df:
            print("----------acquired <silver_dataflow_spec_df> moving forward to onboard silver dataflowspec-----------")

        columns = StructType(
            [
                StructField("target_table", StringType(), True),
                StructField("select_exp", ArrayType(StringType(), True), True),
                StructField(
                    "target_partition_cols", ArrayType(StringType(), True), True
                ),
                StructField("where_clause", ArrayType(StringType(), True), True),
            ]
        )

        emp_rdd = []
        env = dict_obj["env"]
        silver_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )
        print("--------------created silver_transformation_json_df--------------------------")
        # Get transformation JSON paths by accessing the filtered rows directly
        transformation_json_paths = set()  # Use set to avoid duplicates

        for silver_row in silver_rows:
            try:
                # Access the silver configuration directly from the row
                silver_config = silver_row[f"silver_{env}"]
                if silver_config:
                    transformation_json_path = silver_config[f"silver_transformation_json_{env}"]
                    if transformation_json_path and transformation_json_path.strip():
                        transformation_json_paths.add(transformation_json_path)
            except Exception as e:
                logger.warning(f"Could not access transformation JSON path for a row: {e}")
                continue

        logger.info(f"Found transformation JSON paths: {list(transformation_json_paths)}")
        print(f"-----transformation_json_paths: {list(transformation_json_paths)} -------------------")

        # Process each unique transformation JSON file
        for transformation_json_path in transformation_json_paths:
            print("-------------iterating through transformation jsons paths---------------------")
            try:
                logger.info(f"Reading transformation JSON from: {transformation_json_path}")
                transformation_df = self.spark.read.option("multiline", "true").schema(columns).json(transformation_json_path)
                silver_transformation_json_df = silver_transformation_json_df.union(transformation_df)
            except Exception as e:
                logger.error(f"Error reading transformation JSON from {transformation_json_path}: {e}")
                continue

        print("-----out of transformation json paths, joining silver_data_flow_spec df--------")
        silver_data_flow_spec_df = silver_transformation_json_df.join(
            silver_data_flow_spec_df,
            silver_transformation_json_df.target_table
            == silver_data_flow_spec_df.targetDetails["table"],
        )
        print("----going to add select_exp & where_clause-------")
        silver_dataflow_spec_df = (
            silver_data_flow_spec_df.drop("target_table")  # .drop("path")
            .drop("target_partition_cols")
            .withColumnRenamed("select_exp", "selectExp")
            .withColumnRenamed("where_clause", "whereClause")
        )
        print("----adding audit columns-------")
        silver_dataflow_spec_df = self.__add_audit_columns(
            silver_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        silver_fields = [field.name for field in dataclasses.fields(SilverDataflowSpec)]
        silver_dataflow_spec_df = silver_dataflow_spec_df.select(silver_fields)
        print("----silver_dataflow_spec_df created------- Fields: ", silver_dataflow_spec_df.printSchema())

        print("----writing silver_dataflow_spec_df to table or path-------")
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
            print("--------Silver DataFlowSpecTable overwritten")
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
                print(original_dataflow_df.printSchema())
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["silver_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["silver_dataflowspec_path"]
                )
                print(original_dataflow_df.printSchema())
            logger.info("In Merge block for Silver")
            print("In Merge block for silver")
            self.deltaPipelinesInternalTableOps.merge(
                silver_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
            print("--------Silver DataFlowSpecTable Updated as overwrite is False")
        if not self.uc_enabled:
            self.register_silver_dataflow_spec_tables()


    def onboard_gold_dataflow_spec(self):
        """
        Onboard gold dataflow spec.
        Modified to only process rows that have gold layer data.
        """
        if 'gold' not in self.layers_to_onboard:
            logger.info("Gold layer not selected for onboarding, skipping...")
            return
            
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "gold_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.gold_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("gold_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"], self.dict_obj['env']
        )
        
        # Filter rows to only include those with gold data
        gold_rows = []
        for row in onboarding_df.collect():
            if self._has_layer_data(row, 'gold', dict_obj["env"]):
                gold_rows.append(row)
        
        if not gold_rows:
            logger.warning("No rows found with gold layer data, skipping gold onboarding")
            return

        logger.info(f"Processing {len(gold_rows)} rows for gold layer")

        # Create DataFrame from filtered rows
        gold_onboarding_df = self.spark.createDataFrame(gold_rows, onboarding_df.schema)
            
        gold_data_flow_spec_df = self.__get_gold_dataflow_spec_dataframe(
            gold_onboarding_df, dict_obj["env"]
        )
        env = dict_obj["env"]

        columns = StructType([
            StructField("dlt_views",ArrayType(
                StructType([StructField("reference_name",StringType(),True),
                            StructField("sql_condition",StringType(),True)]))),
                            StructField("sources",ArrayType(
                                                        StructType([
                                                            StructField("filter_condition",StringType(),True)
                                                            ,StructField("pii_fields",MapType(StringType(),StringType(),True),True)
                                                            ,StructField("reference_name",StringType(),True)
                                                            # ,StructField("source_catalog",StringType(),True)
                                                            ,StructField(f"source_table_{env}",StringType(),True)
                                                            ,StructField("is_streaming",StringType(),True)
                                                            ])
                                                            )
                                        ),
            StructField("target_table",StringType(),True),
        ])

        emp_rdd = []
        
        gold_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )

        # Get transformation JSON paths from filtered rows
        transformation_json_paths = set()

        for gold_row in gold_rows:
            try:
                gold_config = gold_row[f"gold_{env}"]
                if gold_config:
                    transformation_json_path = gold_config[f"gold_transformation_json_{env}"]
                    if transformation_json_path and transformation_json_path.strip():
                        transformation_json_paths.add(transformation_json_path)
            except Exception as e:
                logger.warning(f"Could not access gold transformation JSON path for a row: {e}")
                continue

        for transformation_json_path in transformation_json_paths:
            try:
                gold_transformation_json_df = gold_transformation_json_df.union(
                    self.__update_goldtransform_schema(transformation_json_path, columns)
                )
            except Exception as e:
                logger.error(f"Error reading gold transformation JSON from {transformation_json_path}: {e}")
                continue

        gold_data_flow_spec_df = gold_transformation_json_df.join(
            gold_data_flow_spec_df,
            gold_transformation_json_df.target_table
            == gold_data_flow_spec_df.targetDetails["table"],
        )
        gold_dataflow_spec_df = (
            gold_data_flow_spec_df.drop("target_table")  # .drop("path")
            .drop("target_partition_cols")
        )

        gold_dataflow_spec_df = self.__add_audit_columns(
            gold_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        gold_fields = [field.name for field in dataclasses.fields(GoldDataflowSpec)]
        gold_dataflow_spec_df = gold_dataflow_spec_df.select(gold_fields)
        database = dict_obj["database"]
        table = dict_obj["gold_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    gold_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                gold_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["gold_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["gold_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["gold_dataflowspec_path"]
                )
            logger.info("In Merge block for Gold")
            self.deltaPipelinesInternalTableOps.merge(
                gold_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_gold_dataflow_spec_tables()

    def __update_goldtransform_schema(self, path, schema):
        """
        This function updates a schema for a data flow specification based on a JSON file.
        Args:
            path ([Path]): The path to the JSON file containing the data to be processed
            schema ([StructType]): The schema parameter is a StructType object that defines the structure of the
                                   DataFrame to be created from the data. It specifies the names and 
                                   data types of the columns in the DataFrame
        Returns:
            A DataFrame object that contains the transformed data from the input JSON file.
        """
        dict_obj = self.gold_dict_obj
        env = dict_obj["env"]
        data = []
        sourcedata = []

        if(path.lower().endswith("yaml") or path.lower().endswith("yml")) :
            yamlBody = self.spark.read.format("text").load(path).collect().pop()["value"]
            rows = self.spark.createDataFrame(data=yaml.safe_load(yamlBody), schema = schema).collect()
        else :
            transformation_df = self.spark.read.option("multiline", "true").schema(schema).json(path)
            rows = transformation_df.collect()
        print("-----------------loaded the gold transformation file--------------------")
        for row in rows:
            dlt_views=row["dlt_views"]
            target_table=row["target_table"]
            pii_fields={}
            sources = row["sources"]
            print("------------iterating through the sources--------------------")
            for source in sources:
                if source:
                    pii_fields = {}
                    print("------------creating the pii fields--------------------")
                    if("pii_fields" in source) :
                        if(type(source["pii_fields"]) is dict):
                            json_pii_fields  = source["pii_fields"]
                        else :
                            json_pii_fields  = source["pii_fields"].asDict()
                        for piiField in json_pii_fields :
                            if(json_pii_fields[piiField]):
                                pii_fields[piiField] = json_pii_fields[piiField]
                    else :
                        pii_fields = {}
                    print("------------filter condition creation-----------------")
                    if("filter_condition" in source) :
                        filter_condition = source["filter_condition"]
                    else :
                        filter_condition = ""

                    reference_name = source["reference_name"]
                    
                    # Updated to support environment-specific source_table
                    print("-----------source_table creation------------")
                    if f"source_table_{env}" in source:
                        source_table_env = source[f"source_table_{env}"]
                    else:
                        logger.warning(f"No source_table_{env} found in source: {source}")
                        source_table_env = ""
                    
                    source_is_streaming = source["is_streaming"] if "is_streaming" in source else "false"
                    print("----------accumulating all the source information----------------")
                    sourceRow = (
                        filter_condition,
                        pii_fields,
                        reference_name,
                        source_table_env,
                        source_is_streaming
                    )
                    sourcedata.append(sourceRow)
            print("--------------creating the dataRow-----------------")
            dataRow = (
                dlt_views,
                sourcedata,
                target_table
            )
            data.append(dataRow)
            print("--------------appending the data to the source data-----------------")
            sourcedata = []
            print("--------resetting the sourcedata-----------------")
        data_flow_spec_rows_df = self.spark.createDataFrame(data, schema)
        print("------------created the dataflow_spec_rows for the gold tranformation---------------------------")
        return data_flow_spec_rows_df

    def __delete_none(self, _dict):
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def __get_onboarding_file_dataframe(self, onboarding_file_path, env):
        onboarding_df = None
        if onboarding_file_path.lower().endswith(".json"):
            onboarding_df = self.spark.read.option("multiline", "true").json(
                onboarding_file_path
            )
            # onboarding_df.show(truncate=False)

            self.onboard_file_type = "json"
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
        """Add_audit_columns method will add AuditColumns like version, dates, author.

        Args:
            df ([type]): [description]
            dict_obj ([type]): attributes = ["import_author", "version"]

        Returns:
            [type]: attributes = ["import_author", "version"]
        """
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

    def __validate_mandatory_fields(self, onboarding_row, mandatory_fields):
        for field in mandatory_fields:
            if not onboarding_row[field]:
                raise Exception(f"Missing field={field} in onboarding_row")

    def __get_bronze_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get bronze dataflow spec method will convert onboarding dataframe to Bronze Dataflowspec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "writerConfigOptions",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "targetPiiFields",
            "isStreaming",
            "flattenNestedData",
            "columnToExtract",
            "version",
            "description",
            "metadata"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
                StructField("readerConfigOptions",MapType(StringType(), StringType(), True),True,),
                StructField("targetFormat", StringType(), True),
                StructField("targetDetails", MapType(StringType(), StringType(), True), True),
                StructField("tableProperties", MapType(StringType(), StringType(), True), True),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("quarantineTargetDetails",MapType(StringType(), StringType(), True),True,),
                StructField("quarantineTableProperties",MapType(StringType(), StringType(), True),True,),
                StructField("writerConfigOptions",MapType(StringType(), StringType(), True),True,),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("targetPiiFields",MapType(StringType(), StringType(), True),True,),
                StructField("isStreaming", StringType(), True),
                StructField("flattenNestedData", StringType(), True),
                StructField("columnToExtract", ArrayType(StringType(), True), True),
                StructField("version", StringType(), True),
                StructField("description", StringType(), True),
                StructField("metadata", StructType([
                    StructField("created_by", StringType(), True),
                    StructField("created_date", StringType(), True),
                    StructField("last_modified", StringType(), True),
                    StructField("business_owner", StringType(), True),
                    StructField("data_classification", StringType(), True),
                    StructField("retention_policy", StringType(), True),
                    StructField("compliance_requirements", StringType(), True),
                    StructField("tags", StringType(), True)
                ]), True)
            ]
        )
        data = []
        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"source_details_{env}",  # Updated to use environment variable
            f"bronze_{env}",          # Updated to use environment variable
            "bronze_reader_options",
        ]
        for onboarding_row in onboarding_rows:
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"bronze_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            bronze_data_flow_spec_id = onboarding_row["data_flow_id"]
            bronze_data_flow_spec_group = onboarding_row["data_flow_group"]
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in [
                "cloudfiles",
                "csv",
                "parquet",
                "json",
                "eventhub",
                "kafka",
                "delta",
                "snapshot"
            ]:
                raise Exception(
                    f"Source format {source_format} not supported in DLT-META! row={onboarding_row}"
                )
            source_details, bronze_reader_config_options, schema = (
                self.get_bronze_source_details_reader_options_schema(
                    onboarding_row, env
                )
            )
            bronze_target_format = "delta"
            bronze_target_details = {
                "database": onboarding_row[f"bronze_{env}"]["target_details"]["catalog"]+"."+onboarding_row[f"bronze_{env}"]["target_details"]["schema"],
                "table": onboarding_row[f"bronze_{env}"]["target_details"]["table"],
            }
            if not self.uc_enabled:
                if "path" in onboarding_row[f"bronze_{env}"]["target_details"]:
                    bronze_target_details["path"] = onboarding_row[f"bronze_{env}"]["target_details"]["path"]
                else:
                    raise Exception(f"bronze_table_path_{env} not provided in onboarding_row={onboarding_row}")
            bronze_table_properties = {}
            if (
                "bronze_table_properties" in onboarding_row[f"bronze_{env}"]
                and onboarding_row[f"bronze_{env}"]["bronze_table_properties"]
            ):
                bronze_table_properties = self.__delete_none(
                    onboarding_row[f"bronze_{env}"]["bronze_table_properties"].asDict()
                )

            partition_columns = [""]
            if (
                "partition_columns" in onboarding_row[f"bronze_{env}"]
                and onboarding_row[f"bronze_{env}"]["partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row[f"bronze_{env}"]["partition_columns"]:
                    partition_columns = onboarding_row[f"bronze_{env}"]["partition_columns"].split(",")
                else:
                    partition_columns = [onboarding_row[f"bronze_{env}"]["partition_columns"]]

            cluster_by = self.__get_cluster_by_properties(onboarding_row, f"bronze_{env}", bronze_table_properties, "cluster_by")

            cdc_apply_changes = None
            if (
                "cdc_apply_changes" in onboarding_row[f"bronze_{env}"]
                and onboarding_row[f"bronze_{env}"]["cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, f"bronze_{env}")
                cdc_apply_changes = json.dumps(
                    self.__delete_none(
                        onboarding_row[f"bronze_{env}"]["cdc_apply_changes"].asDict()
                    )
                )
            apply_changes_from_snapshot = None
            if ("apply_changes_from_snapshot" in onboarding_row[f"bronze_{env}"]
                    and onboarding_row[f"bronze_{env}"]["apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, f"bronze_{env}")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row[f"bronze_{env}"]["apply_changes_from_snapshot"].asDict())
                )
            data_quality_expectations = None
            quarantine_target_details = {}
            quarantine_table_properties = {}
            if f"bronze_data_quality_expectations_json_{env}" in onboarding_row[f"bronze_{env}"]:
                bronze_data_quality_expectations_json = onboarding_row[f"bronze_{env}"][f"bronze_data_quality_expectations_json_{env}"]
                if bronze_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        bronze_data_quality_expectations_json
                    )
            if ("bronze_quarantine_table" in onboarding_row[f"bronze_{env}"] and onboarding_row[f"bronze_{env}"]["bronze_quarantine_table"]):
                quarantine_target_details, quarantine_table_properties = self.__get_quarantine_details(env, onboarding_row, f"bronze_{env}")
            
            writer_config_options = {}
            if (
                "bronze_writer_config_options" in onboarding_row[f"bronze_{env}"] and onboarding_row[f"bronze_{env}"]["bronze_writer_config_options"]
            ):
                writer_config_options = self.__delete_none(
                    onboarding_row[f"bronze_{env}"]["bronze_writer_config_options"].asDict()
                    )
            
            targetPiiFields = {}
            if (
                "target_pii_fields" in onboarding_row[f"bronze_{env}"]
                and onboarding_row[f"bronze_{env}"]["target_pii_fields"]
            ):
                print(onboarding_row[f"bronze_{env}"]["target_pii_fields"])
                targetPiiFields = self.__delete_none(
                    onboarding_row[f"bronze_{env}"]["target_pii_fields"].asDict()
                    )

            append_flows, append_flows_schemas = self.get_append_flows_json(
                onboarding_row, f"bronze_{env}", env
            )
            
            if "is_streaming" in onboarding_row[f"bronze_{env}"] and onboarding_row[f"bronze_{env}"]["is_streaming"]:
                isStreaming = onboarding_row[f"bronze_{env}"]["is_streaming"]
            else:
                isStreaming = None
            
            if "flatten_nested_data" in onboarding_row and onboarding_row["flatten_nested_data"]:
                flattenNestedData = onboarding_row["flatten_nested_data"]
            else:
                flattenNestedData = None

            if "column_to_extract" in onboarding_row and onboarding_row["column_to_extract"]:
                columnToExtract = onboarding_row["column_to_extract"]
            else:
                columnToExtract = None

            if "version" in onboarding_row and onboarding_row["version"]:
                version = onboarding_row["version"]
            else:
                version = None
            
            if "description" in onboarding_row and onboarding_row["description"]:
                description = onboarding_row["description"]
            else:
                description = None

            if "metadata" in onboarding_row and onboarding_row["metadata"]:
                metadata = onboarding_row["metadata"]
            else:
                metadata = None
            

            # ab_translator_config, ab_validation_rules, ab_message_types = self.process_ab_config(self, obnoring_row, env)

            bronze_row = (
                bronze_data_flow_spec_id,
                bronze_data_flow_spec_group,
                source_format,
                source_details,
                bronze_reader_config_options,
                bronze_target_format,
                bronze_target_details,
                bronze_table_properties,
                schema,
                partition_columns,
                cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                quarantine_target_details,
                quarantine_table_properties,
                writer_config_options,
                append_flows,
                append_flows_schemas,
                cluster_by,
                targetPiiFields,
                isStreaming,
                flattenNestedData,
                columnToExtract,
                version,
                description,
                metadata,
                # ab_translator_config,
                # ab_validation_rules,
                # ab_message_types
            )
            data.append(bronze_row)
            # logger.info(bronze_parition_columns)

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __get_cluster_by_properties(self, onboarding_row, layer, table_properties, cluster_key):
        cluster_by = None
        if cluster_key in onboarding_row[f"{layer}"] and onboarding_row[f"{layer}"][cluster_key]:
            if table_properties.get('pipelines.autoOptimize.zOrderCols', None) is not None:
                raise Exception(f"Can not support zOrder and cluster_by together at {cluster_key}")
            cluster_by = onboarding_row[f"{layer}"][cluster_key]
        return cluster_by

    def __get_quarantine_details(self, env, onboarding_row, layer):
        quarantine_table_partition_columns = ""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        quarantine_table_cluster_by = None
        if (
            "bronze_quarantine_table_partitions" in onboarding_row[f"{layer}"]
            and onboarding_row[f"{layer}"]["bronze_quarantine_table_partitions"]
        ):
            # Split if this is a list separated by commas
            if "," in onboarding_row[f"{layer}"]["bronze_quarantine_table_partitions"]:
                quarantine_table_partition_columns = onboarding_row[f"{layer}"]["bronze_quarantine_table_partitions"].split(",")
            else:
                quarantine_table_partition_columns = onboarding_row[f"{layer}"]["bronze_quarantine_table_partitions"]
        if (
            "bronze_quarantine_table_properties" in onboarding_row[f"{layer}"]
            and onboarding_row[f"{layer}"]["bronze_quarantine_table_properties"]
        ):
            quarantine_table_properties = self.__delete_none(
                onboarding_row[f"{layer}"]["bronze_quarantine_table_properties"].asDict()
            )

        quarantine_table_cluster_by = self.__get_cluster_by_properties(onboarding_row, layer, quarantine_table_properties,
                                                                       "bronze_quarantine_table_cluster_by")
        if (
            f"bronze_database_quarantine_{env}" in onboarding_row[f"{layer}"]
            and onboarding_row[f"{layer}"][f"bronze_database_quarantine_{env}"]
        ):
            quarantine_target_details = {"database": onboarding_row[f"{layer}"][f"bronze_database_quarantine_{env}"],
                                         "table": onboarding_row[f"{layer}"]["bronze_quarantine_table"],
                                         "partition_columns": quarantine_table_partition_columns,
                                         "cluster_by": quarantine_table_cluster_by
                                         }
        if not self.uc_enabled and f"bronze_quarantine_table_path_{env}" in onboarding_row[f"{layer}"]:
            quarantine_target_details["path"] = onboarding_row[f"{layer}"][f"bronze_quarantine_table_path_{env}"]

        return quarantine_target_details, quarantine_table_properties

    def get_append_flows_json(self, onboarding_row, layer, env):
        append_flows = None
        append_flows_schema = {}
        if (
            f"{layer}_append_flows" in onboarding_row
            and onboarding_row[f"{layer}_append_flows"]
        ):
            self.__validate_append_flow(onboarding_row, layer)
            json_append_flows = onboarding_row[f"{layer}_append_flows"]
            from pyspark.sql.types import Row

            af_list = []
            for json_append_flow in json_append_flows:
                json_append_flow = json_append_flow.asDict()
                append_flow_map = {}
                for key in json_append_flow.keys():
                    if isinstance(json_append_flow[key], Row):
                        fs = json_append_flow[key].__fields__
                        mp = {}
                        for ff in fs:
                            if f"source_path_{env}" == ff:
                                mp["path"] = json_append_flow[key][f"{ff}"]
                            elif "source_schema_path" == ff:
                                source_schema_path = json_append_flow[key][f"{ff}"]
                                if source_schema_path:
                                    schema = self.__get_bronze_schema(
                                        source_schema_path
                                    )
                                    append_flows_schema[json_append_flow["name"]] = (
                                        schema
                                    )
                            else:
                                mp[f"{ff}"] = json_append_flow[key][f"{ff}"]
                        append_flow_map[key] = self.__delete_none(mp)
                    else:
                        append_flow_map[key] = json_append_flow[key]
                af_list.append(self.__delete_none(append_flow_map))
            append_flows = json.dumps(af_list)
        return append_flows, append_flows_schema

    def __validate_apply_changes(self, onboarding_row, layer):
        cdc_apply_changes = onboarding_row[f"{layer}"]["cdc_apply_changes"]

        # Check if CDC apply changes is actually intended to be used
        if not cdc_apply_changes:
            logger.info(f"CDC apply changes is empty for {layer}, skipping validation")
            return

        json_cdc_apply_changes = self.__delete_none(cdc_apply_changes.asDict())
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")

        if not json_cdc_apply_changes:
            logger.info(f"CDC apply changes has no valid values for {layer}, skipping validation")
            return
        else:
            logger.info(f"cdc apply changes is not empty for {layer}, validating")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(
            DataflowSpecUtils.cdc_applychanges_api_attributes
        ).difference(payload_keys)
        logger.info(
            f"""missing cdc payload keys:{missing_cdc_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
            payload_keys
        ):
            missing_mandatory_attr = set(
                DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes
            ) - set(payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_cdc_apply_changes = {missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_cdc_apply_changes atrributes
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        apply_changes_from_snapshot = onboarding_row[f"{layer}"]["apply_changes_from_snapshot"]
        # Check if CDC apply changes is actually intended to be used
        if not cdc_apply_changes:
            logger.info(f"CDC apply changes is empty for {layer}, skipping validation")
            return
        json_apply_changes_from_snapshot = self.__delete_none(apply_changes_from_snapshot.asDict())
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        if not json_cdc_apply_changes:
            logger.info(f"CDC apply changes has no valid values for {layer}, skipping validation")
            return
        else:
            logger.info(f"cdc apply changes is not empty for {layer}, validating")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = (
            set(DataflowSpecUtils.apply_changes_from_snapshot_api_attributes).difference(payload_keys)
        )
        logger.info(
            f"""missing applyChangesFromSnapshot payload keys:{missing_apply_changes_from_snapshot_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_apply_changes_from_snapshot = {
                missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_apply_changes_from_snapshot atrributes
                 {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

    def get_bronze_source_details_reader_options_schema(self, onboarding_row, env):
        """Get bronze source reader options.

        Args:
            onboarding_row ([type]): [description]

        Returns:
            [type]: [description]
        """
        source_details = {}
        bronze_reader_config_options = {}
        schema = None
        source_format = onboarding_row["source_format"]
        bronze_reader_options_json = (
            onboarding_row["bronze_reader_options"]
            if "bronze_reader_options" in onboarding_row
            else {}
        )
        if bronze_reader_options_json:
            bronze_reader_config_options = self.__delete_none(
                bronze_reader_options_json.asDict()
            )
        source_details_json = onboarding_row[f"source_details_{env}"]  # Updated to use environment variable
        if source_details_json:
            source_details_file = self.__delete_none(source_details_json.asDict())
            if (source_format.lower() == "cloudfiles"
                    or source_format.lower() == "delta"
                    or source_format.lower() == "snapshot"
                    or source_format.lower() == "csv"
                    or source_format.lower() == "json"
                    or source_format.lower() == "parquet"):
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                if "source_database" in source_details_file:
                    source_details["source_database"] = source_details_file[
                        "source_database"
                    ]
                if "source_table" in source_details_file:
                    source_details["source_table"] = source_details_file["source_table"]
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
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
            elif source_format.lower() == "eventhub" or source_format.lower() == "kafka":
                source_details = source_details_file
            elif source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                else:
                    raise Exception(f"source_path_{env} is missing in the source_details")
            if "source_schema_path" in source_details_file:
                source_schema_path = source_details_file["source_schema_path"]
                if source_schema_path:
                    if self.bronze_schema_mapper is not None:
                        schema = self.bronze_schema_mapper(
                            source_schema_path, self.spark
                        )
                    else:
                        schema = self.__get_bronze_schema(source_schema_path)
                else:
                    logger.info(f"no input schema provided for row={onboarding_row}")
                logger.info("spark_schmea={}".format(schema))

        return source_details, bronze_reader_config_options, schema

    def __validate_append_flow(self, onboarding_row, layer):
        append_flows = onboarding_row[f"{layer}_append_flows"]
        for append_flow in append_flows:
            json_append_flow = append_flow.asDict()
            logger.info(f"actual appendFlow={json_append_flow}")
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = set(
                DataflowSpecUtils.append_flow_api_attributes_defaults
            ).difference(payload_keys)
            logger.info(
                f"""missing append flow payload keys:{missing_append_flow_payload_keys}
                    for onboarding row = {onboarding_row}"""
            )
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(
                payload_keys
            ):
                missing_mandatory_attr = set(
                    DataflowSpecUtils.append_flow_mandatory_attributes
                ) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(
                    f"""mandatory missing atrributes for {layer}_append_flow = {missing_mandatory_attr}
                    for onboarding row = {onboarding_row}"""
                )
            else:
                logger.info(
                    f"""all mandatory {layer}_append_flow atrributes
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

    def __get_data_quality_expecations(self, json_file_path):
        """Get Data Quality expections from json file.

        Args:
            json_file_path ([type]): [description]
        """
        json_string = None
        if json_file_path and json_file_path.endswith(".json"):
            expectations_df = self.spark.read.text(json_file_path, wholetext=True)
            expectations_arr = expectations_df.collect()
            if len(expectations_arr) == 1:
                json_string = expectations_df.collect()[0]["value"]
        return json_string

    def __get_silver_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get silver_dataflow_spec method transform onboarding dataframe to silver dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "writerConfigOptions",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "targetPiiFields"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField(
                    "writerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("targetPiiFields", MapType(StringType(), StringType(), True), True)
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"silver_{env}",  # Updated to use consolidated silver configuration
        ]

        for onboarding_row in onboarding_rows:
            self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            
            silver_data_flow_spec_id = onboarding_row["data_flow_id"]
            silver_data_flow_spec_group = onboarding_row["data_flow_group"]
            silver_reader_config_options = {}

            silver_target_format = "delta"

            # Gather source_details from silver configuration if available
            if "source_details" in onboarding_row[f"silver_{env}"]:
                source_details = onboarding_row[f"silver_{env}"]["source_details"]
                silver_source_details = {
                    "database": source_details["catalog"] + "." + source_details["schema"],
                    "table": source_details["table"],
                }
            elif f"bronze_{env}" in onboarding_row and onboarding_row[f"bronze_{env}"]:
                # Use bronze table from same row as source (combined bronze+silver flow)
                bronze_config = onboarding_row[f"bronze_{env}"]
                silver_source_details = {
                    "database": bronze_config["target_details"]["catalog"] + "." + bronze_config["target_details"]["schema"],
                    "table": bronze_config["target_details"]["table"],
                }
            else:
                raise Exception(f"Either silver source_details or bronze_{env} configuration required. No valid silver source details found for silver layer in row={onboarding_row}")

            # Extract silver target details from silver_{env} configuration
            silver_target_details = {
                "database": onboarding_row[f"silver_{env}"]["target_details"]["catalog"] + "." + onboarding_row[f"silver_{env}"]["target_details"]["schema"],
                "table": onboarding_row[f"silver_{env}"]["target_details"]["table"],
            }

            if not self.uc_enabled and "path" in onboarding_row[f"silver_{env}"]["target_details"]:
                silver_target_details["path"] = onboarding_row[f"silver_{env}"]["target_details"]["path"]

            silver_table_properties = {}
            if (
                "silver_table_properties" in onboarding_row[f"silver_{env}"]
                and onboarding_row[f"silver_{env}"]["silver_table_properties"]
            ):
                silver_table_properties = self.__delete_none(
                    onboarding_row[f"silver_{env}"]["silver_table_properties"].asDict()
                )

            silver_partition_columns = [""]
            if (
                "partition_columns" in onboarding_row[f"silver_{env}"]
                and onboarding_row[f"silver_{env}"]["partition_columns"]
            ):
                # Handle list or comma-separated string
                partition_cols = onboarding_row[f"silver_{env}"]["partition_columns"]
                if isinstance(partition_cols, list):
                    silver_partition_columns = partition_cols
                elif isinstance(partition_cols, str) and "," in partition_cols:
                    silver_partition_columns = partition_cols.split(",")
                elif isinstance(partition_cols, str):
                    silver_partition_columns = [partition_cols]

            silver_cluster_by = self.__get_cluster_by_properties(onboarding_row, f"silver_{env}", silver_table_properties, "cluster_by")

            silver_cdc_apply_changes = None
            if (
                "cdc_apply_changes" in onboarding_row[f"silver_{env}"]
                and onboarding_row[f"silver_{env}"]["cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, f"silver_{env}")
                silver_cdc_apply_changes_row = onboarding_row[f"silver_{env}"]["cdc_apply_changes"]
                if self.onboard_file_type == "json":
                    silver_cdc_apply_changes = json.dumps(
                        self.__delete_none(silver_cdc_apply_changes_row.asDict())
                    )

            writer_config_options = {}
            if (
                "silver_writer_config_options" in onboarding_row[f"silver_{env}"] and onboarding_row[f"silver_{env}"]["silver_writer_config_options"]
            ):
                writer_config_options = self.__delete_none(
                    onboarding_row[f"silver_{env}"]["silver_writer_config_options"].asDict()
                    )
            
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, f"silver_{env}", env
            )
            
            target_PiiFields = {}
            if (
                "target_pii_fields" in onboarding_row[f"silver_{env}"]
                and onboarding_row[f"silver_{env}"]["target_pii_fields"]
            ):
                target_PiiFields = self.__delete_none(
                    onboarding_row[f"silver_{env}"]["target_pii_fields"].asDict()
                )

            silver_row = (
                silver_data_flow_spec_id,
                silver_data_flow_spec_group,
                "delta",
                silver_source_details,
                silver_reader_config_options,
                silver_target_format,
                silver_target_details,
                silver_table_properties,
                silver_partition_columns,
                silver_cdc_apply_changes,
                writer_config_options,
                append_flows,
                append_flow_schemas,
                silver_cluster_by,
                target_PiiFields
            )
            data.append(silver_row)
            logger.info(f"silver_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df

    def __get_gold_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get gold_dataflow_spec method transform onboarding dataframe to gold dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "isStreaming", 
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "dataQualityExpectations",
            "writerConfigOptions",
            "appendFlows", 
            "appendFlowsSchemas",
            "clusterBy",
            "targetPiiFields",
            "env"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("isStreaming", StringType(), True),
                StructField("targetFormat", StringType(), True),
                StructField("targetDetails", MapType(StringType(), StringType(), True), True),
                StructField("tableProperties", MapType(StringType(), StringType(), True), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("writerConfigOptions",MapType(StringType(), StringType(), True),True,),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("targetPiiFields",MapType(StringType(), StringType(), True),True,),
                StructField("env", StringType(), True)
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"gold_{env}",  # Updated to use consolidated gold configuration
        ]

        for onboarding_row in onboarding_rows:
            self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            
            gold_data_flow_spec_id = onboarding_row["data_flow_id"]
            gold_data_flow_spec_group = onboarding_row["data_flow_group"]
            
            # Extract is_streaming from gold configuration or default to false
            gold_is_streaming = "false"
            if (
                "is_streaming" in onboarding_row[f"gold_{env}"]
                and onboarding_row[f"gold_{env}"]["is_streaming"]
            ):
                gold_is_streaming = str(onboarding_row[f"gold_{env}"]["is_streaming"]).lower()

            gold_target_format = "delta"

            # Extract gold target details from gold_{env} configuration
            gold_target_details = {
                "database": onboarding_row[f"gold_{env}"]["target_details"]["catalog"] + "." + onboarding_row[f"gold_{env}"]["target_details"]["schema"],
                "table": onboarding_row[f"gold_{env}"]["target_details"]["table"],
            }

            if not self.uc_enabled and "path" in onboarding_row[f"gold_{env}"]["target_details"]:
                gold_target_details["path"] = onboarding_row[f"gold_{env}"]["target_details"]["path"]

            gold_table_properties = {}
            if (
                "gold_table_properties" in onboarding_row[f"gold_{env}"]
                and onboarding_row[f"gold_{env}"]["gold_table_properties"]
            ):
                gold_table_properties = self.__delete_none(
                    onboarding_row[f"gold_{env}"]["gold_table_properties"].asDict()
                )

            gold_partition_columns = [""]
            if (
                "partition_columns" in onboarding_row[f"gold_{env}"]
                and onboarding_row[f"gold_{env}"]["partition_columns"]
            ):
                # Handle list or comma-separated string
                partition_cols = onboarding_row[f"gold_{env}"]["partition_columns"]
                if isinstance(partition_cols, list):
                    gold_partition_columns = partition_cols
                elif isinstance(partition_cols, str) and "," in partition_cols:
                    gold_partition_columns = partition_cols.split(",")
                elif isinstance(partition_cols, str):
                    gold_partition_columns = [partition_cols]

            gold_cluster_by = self.__get_cluster_by_properties(onboarding_row, f"gold_{env}", gold_table_properties, "cluster_by")

            gold_cdc_apply_changes = None
            if (
                "cdc_apply_changes" in onboarding_row[f"gold_{env}"]
                and onboarding_row[f"gold_{env}"]["cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, f"gold_{env}")
                gold_cdc_apply_changes_row = onboarding_row[f"gold_{env}"]["cdc_apply_changes"]
                if self.onboard_file_type == "json":
                    gold_cdc_apply_changes = json.dumps(
                        self.__delete_none(gold_cdc_apply_changes_row.asDict())
                    )

            data_quality_expectations = None
            if f"gold_data_quality_expectations_json_{env}" in onboarding_row[f"gold_{env}"]:
                gold_data_quality_expectations_json = onboarding_row[f"gold_{env}"][f"gold_data_quality_expectations_json_{env}"]
                if gold_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        gold_data_quality_expectations_json
                    )
            writer_config_options = {}
            if (
                "gold_writer_config_options" in onboarding_row[f"gold_{env}"] and onboarding_row[f"gold_{env}"]["gold_writer_config_options"]
            ):
                writer_config_options = self.__delete_none(
                    onboarding_row[f"gold_{env}"]["gold_writer_config_options"].asDict()
                    )
                
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, f"gold_{env}", env
            )

            targetPiiFields = {}
            if (
                "target_pii_fields" in onboarding_row[f"gold_{env}"]
                and onboarding_row[f"gold_{env}"]["target_pii_fields"]
            ):
                targetPiiFields = self.__delete_none(
                    onboarding_row[f"gold_{env}"]["target_pii_fields"].asDict()
                )

            gold_row = (
                gold_data_flow_spec_id,
                gold_data_flow_spec_group,
                gold_is_streaming,
                gold_target_format,
                gold_target_details,
                gold_table_properties,
                gold_partition_columns,
                gold_cdc_apply_changes,
                data_quality_expectations,
                writer_config_options,
                append_flows,
                append_flow_schemas,
                gold_cluster_by,
                targetPiiFields,
                env
            )
            data.append(gold_row)
            logger.info(f"gold_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df