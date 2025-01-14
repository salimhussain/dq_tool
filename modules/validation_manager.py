from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


def get_latest_execution_key() -> tuple:
    """Fetch the latest execution_key from the specified table.

    :return: A integer representing the latest validation_key and execution_key.
    """
    # Initialize Spark session
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()
    print("[*] Getting latest execution key")
    
    # Set the schema
    query = f"SELECT validation_key, execution_key FROM dqops.dq_validations ORDER BY validation_key DESC LIMIT 1"

    # Execute the query and collect the result to the driver
    result_df = spark.sql(query)
    result = result_df.collect()

    if result:
        return result[0]["validation_key"], result[0]["execution_key"]
    else:
        logger.info(f"No validation found for validations table")
        return 0, 0


class ValidationManager:

    @staticmethod
    def run(context_manager, data_sources: Optional[List[str]] = None):
        """Run validations for the specified data sources. If no data sources are provided, validate all.

        :param context_manager: An instance of GXContextManager managing the context and data assets.
        :param data_sources: List of data source names to validate. Validates all if None.
        :return: The latest execution_key
        """

        validation_key, execution_key = get_latest_execution_key()
        print(
            f"[*] Running validations. Latest validation key: {validation_key}, Execution key: {execution_key}"
        )

        execution_key += 1

        for checkpoint in context_manager.context.checkpoints.all():
            try:
                # Extract data source name from the checkpoint
                data_source = checkpoint.name.split("-", 1)[1]

                # If data_sources is specified, skip if the current data_source is not in the list
                if data_sources and data_source not in data_sources:
                    print(f"Skipping data source: {data_source}")
                    continue

                # Retrieve the corresponding data asset
                data_asset = context_manager.data_assets.get(data_source)

                if not data_asset:
                    logger.warning(
                        f"No data asset found for data source: {data_source}"
                    )
                    continue

                print(f"[*] Validating data source: {data_source}")
                # Run the validation using the checkpoint
                results = checkpoint.run(
                    batch_parameters={"dataframe": data_asset.dataframe}
                )
                
                print(f"[*] Validations completed for data source: {data_source}")
                
                results = results.run_results[list(results.run_results.keys())[0]]

                print(f"[*] Validation results: {len(results.results)}")
                
                # Parse and store validation results in the data asset
                data_asset.parse_results(
                    results["results"],
                    validation_key,
                    execution_key,
                )
                
                # Increment the validation_key by the number of rules
                validation_key += data_asset.num_rules

            except Exception as e:
                print(f"Error validating data source {data_source}: {e}")
                raise Exception
