from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    LongType,
    TimestampType,
    DateType,
)

DQ_RULE_TYPE_SCHEMA = StructType(
    [
        StructField(
            "integration_id",
            StringType(),
            nullable=False,
            metadata={
                "primary_key": True,
                "description": "Combination of rule_type_key~rule_type_name",
            },
        ),
        StructField(
            "rule_type_key",
            IntegerType(),
            nullable=False,
            metadata={
                "primary_key": True,
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
