from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

BRNZ_DQ_RULE_TYPE_SCHEMA = StructType(
    [
        StructField(
            "rule_type_key",
            IntegerType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "description": "Unique identifier for the rule type.",
            },
        ),
        StructField("rule_type_name", StringType(), nullable=False),
        StructField("dq_dimension", StringType(), nullable=False),
        StructField("gx_expectation", StringType(), nullable=False),
        StructField("gx_description", StringType(), nullable=False),
        StructField("created_at_ts", TimestampType(), nullable=False),
        StructField("modified_at_ts", TimestampType(), nullable=False),
    ]
)
