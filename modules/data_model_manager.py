from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, coalesce

from typing import Dict, List
from delta.tables import DeltaTable

from schemas.data_model.dq_exceptions_schema import DQ_EXCEPTIONS_SCHEMA
from schemas.data_model.dq_validations_schema import DQ_VALIDATIONS_SCHEMA

import logging

logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()


def align_to_schema(
    df: DataFrame, schema: StructType = DQ_EXCEPTIONS_SCHEMA
) -> DataFrame:
    """Align a DataFrame to the specified schema by adding missing columns with null values and,
    reordering the columns to match the schema.

    :param df: The DataFrame to align.
    :param schema: The schema to align to.
    """
    columns = df.columns
    for field in schema.fields:
        if field.name not in columns:
            print(f"\t- Adding missing column {field.name} to DataFrame")
            df = df.withColumn(
                field.name, lit(None).cast(field.dataType)
            )  # Add missing column
        # Cast the column to the correct type if doesn't match the schema
        if field.dataType != df.select(col(field.name)).schema[field.name].dataType:
            print(f"\t- Casting column {field.name} to {field.dataType}")
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df.select([field.name for field in schema.fields])  # Reorder columns


class DataModelManager:
    """A class to manage the integration of rules, validations, and exception into the data model tables."""

    DQ_RULE = "dqops.dq_rule"
    DQ_RULE_TYPE = "dqops.dq_rule_type"
    DQ_EXCEPTIONS = "dqops.dq_exceptions"
    DQ_VALIDATIONS = "dqops.dq_validations"

    def merge_in_table(
        self,
        df: DataFrame,
        columns: list,
        primary_key: str = "integration_id",
        destination_table: str = "dqops.dq_rule",
    ) -> None:
        """Generic method to store data into a Delta table with upsert or insert-only logic.

        :param data: List of dictionaries to be stored.
        :param schema: The schema of the table.
        :param table_path: Path to the Delta table.
        :param primary_key: The primary key column for upsert logic.
        :param destination_table: The destination table to upsert into.
        """
        try:
            print(f"\t- Storing data into {destination_table}...")
            delta_table = DeltaTable.forName(spark, destination_table)
            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{primary_key} = source.{primary_key}",
                )
                .whenMatchedUpdate(set={col: f"source.{col}" for col in columns})
                .whenNotMatchedInsert(values={col: f"source.{col}" for col in columns})
                .execute()
            )
            print(f"\t- Data stored in {destination_table}.")
        except Exception as e:
            raise ValueError(
                f"Error upserting data into table {destination_table}: {e}"
            )

    def _store_in_table(self, df: DataFrame, table_path: str) -> None:
        """Store data into a Delta table by appending new rows.

        :param df: Spark DataFrame containing the data to store.
        :param table_path: Path to the Delta table.
        """
        try:
            print(f"\t- Storing data into {table_path}...")
            df.write.format("delta").mode("append").saveAsTable(table_path)
            print(f"\t- Data stored in {table_path}.")
        except Exception as e:
            raise ValueError(f"Error storing data in Delta table {table_path}: {e}")

    def save_expectation_suite(self, expectations: list) -> None:
        """Save the expectation suite to the data model.
        :param expectations: List of expectations.
        """
        validations = []
        for expectation in expectations:
            if hasattr(expectation, 'validation'):
                if not expectation.validation:
                    print(f"[!!!] Skipping expectation {expectation.rule_key} with no validation.")
                    continue
            else:
                print(f"[!!!] Attribute 'validation' does not exist for expectation with rule_key: {expectation.rule_key}.")
                continue
            
            validation = expectation.validation
            exceptions = validation.exceptions

            current_exception_count = 0
            fixed_exception_count = 0
            new_exception_count = 0

            if exceptions:
                print(f"\t[*] Processing {len(exceptions)} exceptions for rule", validation.rule_key)
                
                exception_comparison = self.get_exception_comparison(
                    failed_rows=exceptions,
                    rule_key=validation.rule_key,
                    val_key=validation.validation_key,
                    execution_key=expectation.execution_key,
                )

                current_exception_df = exception_comparison["current_exception"]
                fixed_exception_df = exception_comparison["fixed_exception"]
                new_exception_df = exception_comparison["new_exception"]

                # Combine current and fixed exception into one DataFrame
                all_exception_df = current_exception_df.unionByName(
                    fixed_exception_df, allowMissingColumns=False
                ).unionByName(new_exception_df, allowMissingColumns=False)

                current_exception_count = current_exception_df.count()
                fixed_exception_count = fixed_exception_df.count()
                new_exception_count = new_exception_df.count()
                
                self._store_in_table(all_exception_df, self.DQ_EXCEPTIONS)
                
                print(
                    f"\t- Stored {current_exception_count + fixed_exception_count + new_exception_count} exceptions: "
                    f"{new_exception_count} new, {fixed_exception_count} fixed, "
                    f"{current_exception_count} known \n"
                )

            total_element_count = (
                expectation.element_count or 1
            )  # Avoid division by zero

            validation.update(
                {
                    "known_exception_count": current_exception_count,
                    "new_exception_count": new_exception_count,
                    "fixed_exception_count": fixed_exception_count,
                    "known_exception_percent": current_exception_count
                    / total_element_count,
                    "new_exception_percent": new_exception_count / total_element_count,
                    "fixed_exception_percent": fixed_exception_count
                    / total_element_count,
                }
            )
            # Add the updated validation to the list
            validations.append(validation.to_dict())

        if validations:
            
            print(f"\t[*] Storing {len(validations)} validations in the data model.")
            print("\t[*] Sample of validation:", validations[0])
            
            validations_df = spark.createDataFrame(
                validations, DQ_VALIDATIONS_SCHEMA
            )
            self._store_in_table(validations_df, self.DQ_VALIDATIONS)

    def get_exception_comparison(
        self, failed_rows: List[dict], rule_key: int, val_key: int, execution_key: int
    ) -> Dict[str, DataFrame]:
        """Compare the current failed rows with the latest exception from dq_exception to identify fixed and new exception.

        :param failed_rows: List of dictionaries representing the current failed rows.
        :param expectation_keys: List of expectation keys to filter the dq_exception table.
        :param table_path: Path to the dq_exception table.
        :return: A dictionary with two lists: 'fixed_exception' and 'new_exception'.
        """

        print(f"\t- Getting new, fixed and known exceptions for rule {rule_key} and validation {val_key}")

        # Create a DataFrame from the current failed rows
        current_failed_df = spark.createDataFrame(failed_rows, DQ_EXCEPTIONS_SCHEMA)
        # Print the first row of the DataFrame to check the schema
        current_failed_df.show(1, truncate=False, vertical=True)

        # Get the latest exception from dq_exception
        latest_exception_df = (
            spark.read.format("delta")
            .table(self.DQ_EXCEPTIONS)
            .filter(
                (col("execution_key") == (execution_key - 1))  # Current execution key minus 1
                & (col("rule_key") == rule_key)
                & (col("fixed_exception_flag") == False)
            )
        )

        # Skip further processing if there is no latest exception
        if latest_exception_df.isEmpty():
            print("No latest exception found for rule", rule_key)
            # Return the current failed rows as new exception as there is no latest exception
            return {
                "fixed_exception": spark.createDataFrame([], DQ_EXCEPTIONS_SCHEMA),
                "current_exception": spark.createDataFrame([], DQ_EXCEPTIONS_SCHEMA),
                "new_exception": align_to_schema(current_failed_df.withColumn("new_exception_flag", lit(True)), DQ_EXCEPTIONS_SCHEMA),
            }

        # Update the validation_key for latest exception
        latest_exception_df = latest_exception_df.join(
            current_failed_df.select("rule_key", "validation_key").distinct(),
            on="rule_key",
            how="left",
        ).drop(
            latest_exception_df["validation_key"]
        )  # Drop old validation_key to replace it

        # # Identify fixed exceptions: in latest exception but not in current failed rows
        fixed_exception_df = latest_exception_df.join(
            current_failed_df,
            on=["rule_key", "exception_row_pk"],
            how="left_anti",  # Anti-join to find exception not in current failed rows
        ).withColumn("fixed_exception_flag", lit(True))
        
        # Identify new exception: in current failed rows but not in latest exception
        new_exception_df = current_failed_df.join(
            latest_exception_df.select("rule_key", "exception_row_pk"),  # Match keys
            on=["rule_key", "exception_row_pk"],
            how="left_anti",  # Exclude rows in latest_exception_df
        ).withColumn("new_exception_flag", lit(True))

        # Remove new exception from current exception to avoid duplicates
        # Update the field CREATED_AT_TS for known exception with the ts from the latest exception
        current_exception_df = (
            current_failed_df.join(
                new_exception_df.select("rule_key", "exception_row_pk"),  # Match keys
                on=["rule_key", "exception_row_pk"],
                how="left_anti",  # Exclude rows in new_exception_df
            )
            .join(
                latest_exception_df.select(
                    "rule_key",
                    "exception_row_pk",
                    col("created_at_ts").alias("latest_created_at_ts"),
                ),
                on=["rule_key", "exception_row_pk"],  # Join on key fields
                how="left",
            )
            .withColumn(
                "created_at_ts",
                coalesce(col("latest_created_at_ts"), col("created_at_ts")),
            )
            .drop("latest_created_at_ts")
        )

        print("\t- Finished comparing failed rows. Aligning DataFrames to schema...")
        
        fixed_exception_df = align_to_schema(fixed_exception_df, DQ_EXCEPTIONS_SCHEMA)
        new_exception_df = align_to_schema(new_exception_df, DQ_EXCEPTIONS_SCHEMA)
        current_exception_df = align_to_schema(current_exception_df, DQ_EXCEPTIONS_SCHEMA)

        return {
            "fixed_exception": fixed_exception_df,
            "current_exception": current_exception_df,
            "new_exception": new_exception_df,
        }
