from modules.expectation_manager import ExpectationManager
from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()

# Maximum number of rows to load from a table
MAX_TABLE_ROWS = 1000

class DataAsset:

    __slots__ = [
        "catalog",
        "schema",
        "table",
        "dataframe",
        "element_count",
        "expectation_manager",
        "num_rules",
    ]

    def __init__(self, catalog: str, schema: str, table: str) -> None:

        self.catalog = catalog
        self.schema = schema
        self.table = table

        self.dataframe = self._load_dataframe()
        self.element_count = self.dataframe.count()
        self.expectation_manager = ExpectationManager()

        self.num_rules = 0

    def get_expectation(self, data) -> None:
        expectation, enabled = self.expectation_manager.get_expectation(data)
        if enabled:
            self.num_rules += 1
        return expectation, enabled
    
    def add_expectation(self, expectation) -> None:
        self.expectation_manager.add_expectation(expectation)
    
    def save_expectation_suite(self) -> None:
        self.expectation_manager.save_expectation_suite()

    def parse_results(
        self, results: dict, vaLidation_key: int, execution_key: int
    ) -> None:
        print(f"Parsing results for table {self.table}")
        self.expectation_manager.parse_results(
            results, self.element_count, vaLidation_key, execution_key
        )

    def _load_dataframe(self):
        try:
            df = spark.read.table(f"{self.catalog}.{self.schema}.{self.table}").limit(MAX_TABLE_ROWS)
            print(f"DataFrame loaded for table {self.table}")
            return df
        except Exception as e:
            logger.error(f"Error loading DataFrame: {e}")
            return None
