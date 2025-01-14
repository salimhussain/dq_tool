import pytest

from pyspark.sql import SparkSession
from pyspark.sql import Row

from dq_tool.modules.rule_processor import RuleProcessor


# Initialize a Spark session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestRuleProcessor").getOrCreate()


# Mock data for rules tables
@pytest.fixture
def rules_tables_data(spark):
    data1 = [
        Row(
            TABLE_NAME="table1",
            COLUMN="col1",
            RULE_NO=1,
            RULE_TYPE_NAME="type1",
            CREATED_AT_DATE="12/01/2023",
            MODIFIED_AT_DATE="12/01/2023",
        ),
        Row(
            TABLE_NAME="table2",
            COLUMN="col2",
            RULE_NO=2,
            RULE_TYPE_NAME="type2",
            CREATED_AT_DATE="12/01/2023",
            MODIFIED_AT_DATE="12/01/2023",
        ),
    ]
    data2 = [
        Row(
            TABLE_NAME="table3",
            COLUMN="col3",
            RULE_NO=3,
            RULE_TYPE_NAME="type3",
            CREATED_AT_DATE="12/01/2023",
            MODIFIED_AT_DATE="12/01/2023",
        ),
    ]
    table1 = spark.createDataFrame(data1)
    table2 = spark.createDataFrame(data2)

    return {
        "bronze.brz_dq_rules_stream1": table1,
        "bronze.brz_dq_rules_stream2": table2,
    }


# Mock data for rule types
@pytest.fixture
def rule_types_data(spark):
    data = [
        Row(RULE_TYPE_NAME="type1", RULE_TYPE_KEY=101, GX_EXPECTATION="expectation1"),
        Row(RULE_TYPE_NAME="type2", RULE_TYPE_KEY=102, GX_EXPECTATION="expectation2"),
        Row(RULE_TYPE_NAME="type3", RULE_TYPE_KEY=103, GX_EXPECTATION="expectation3"),
    ]
    return spark.createDataFrame(data)


# Mock the read.table function
def mock_read_table(table_name, mock_tables):
    return mock_tables[table_name]


# Test the RuleProcessor
def test_process_rules(spark, rules_tables_data, rule_types_data, monkeypatch):
    # Mock the spark.read.table method
    def mock_read_table_side_effect(*args, **kwargs):
        table_name = args[1]  # Extract the table name
        if table_name == "gold.gld_dim_rule_type":
            return rule_types_data
        elif table_name in rules_tables_data:
            return rules_tables_data[table_name]
        else:
            raise ValueError(f"Table {table_name} not found")

    monkeypatch.setattr(spark.read, "table", mock_read_table_side_effect)
    monkeypatch.setattr(
        "pyspark.sql.readwriter.DataFrameReader.table", mock_read_table_side_effect
    )

    # Initialize RuleProcessor
    processor = RuleProcessor(spark)

    # List of mock tables
    rules_tables = ["bronze.brz_dq_rules_stream1", "bronze.brz_dq_rules_stream2"]

    # Process rules
    result = processor.process_rules(rules_tables)

    columns = result.columns
    # Assert the result schema and content
    assert "STREAM_NAME" in columns
    assert "INTEGRATION_ID" in columns
    assert "RULE_TYPE_KEY" in columns
    assert "RULE_KEY" in columns
    assert "CREATED_AT_TS" in columns
    assert "MODIFIED_AT_TS" in columns

    # Show result for debugging
    result.show()
