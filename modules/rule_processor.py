from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession
from functools import reduce

import logging

logger = logging.getLogger(__name__)


class RuleProcessor:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process_rules(self, rules_tables: list) -> DataFrame:
        """Process and update rules by:
        - Adding integration_id
        - Joining with dim_rule_type to add gx_expectation
        - Adding rule_key
        - Updating date to timestamps

        :param rules_tables: List of table names containing rules.
        :return: A processed DataFrame with all necessary updates.
        """

        print("[*] Processing rules")

        # Helper to read and annotate tables with STREAM_NAME
        def read_table(table):
            print(f"[*] Reading table: {table}")
            df = self.spark.read.table(table)
            return df.filter((df.current_flag == True) & (df.rule_no.isNotNull()) & (df.created_at_date.isNotNull()))

        # Read and union all tables
        rules = reduce(
            lambda acc, table: acc.union(read_table(table)),
            rules_tables[1:],
            read_table(rules_tables[0]),
        )

        print(f"[*] Read {rules.count()} rules")

        # Join with rule types to add RULE_TYPE_KEY
        rule_types = self.spark.read.table("dqops.dq_rule_type")
        rules = rules.join(
            rule_types.select("rule_type_key", "gx_expectation"),
            on="rule_type_key",
            how="left",
        )

        print(f"[*] Joined with rule types")

        # Add multiple columns and update timestamps
        rules = (
            rules.withColumn(
                "integration_id",
                F.concat_ws("~", F.col("table_name"), F.col("column"), F.col("rule_no").cast("string"), F.col("data_domain"))
            )
            .withColumn(
                "created_at_ts", F.to_timestamp(F.col("created_at_date"), "dd/MM/yyyy")
            )
            .withColumn(
                "modified_at_ts",
                F.to_timestamp(F.col("modified_at_date"), "dd/MM/yyyy"),
            )
            .withColumn("start_date", F.to_date(F.col("start_date"), "dd/MM/yyyy"))
            .withColumn("expiry_date", F.to_date(F.col("expiry_date"), "dd/MM/yyyy"))
            .drop("created_at_date", "modified_at_date")
        )

        print(f"[*] Added integration_id and updated timestamps")
        return rules
