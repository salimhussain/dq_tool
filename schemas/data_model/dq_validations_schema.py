from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    TimestampType,
)

DQ_VALIDATIONS_SCHEMA = StructType(
    [
        StructField(
            "validation_key",
            LongType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "description": "Unique identifier for the validation. Auto-incremented",
            },
        ),
        StructField(
            "rule_key",
            LongType(),
            nullable=False,
            metadata={"description": "Foreign key to the rule"},
        ),
        StructField(
            "execution_key",
            IntegerType(),
            nullable=False,
            metadata={"description": "Number of times the rule has been executed"},
        ),
        StructField("success_flag", BooleanType(), nullable=False),
        StructField("element_count", IntegerType(), nullable=False),
        StructField("exception_count", IntegerType(), nullable=False),
        StructField("known_exception_count", IntegerType(), nullable=False),
        StructField("new_exception_count", IntegerType(), nullable=False),
        StructField("fixed_exception_count", IntegerType(), nullable=False),
        StructField("exception_percent", FloatType(), nullable=False),
        StructField("known_exception_percent", FloatType(), nullable=False),
        StructField("new_exception_percent", FloatType(), nullable=False),
        StructField("fixed_exception_percent", FloatType(), nullable=False),
        StructField("observed_value", StringType(), nullable=True),
        StructField("missing_count", IntegerType(), nullable=False),
        StructField("missing_percent", FloatType(), nullable=False),
        StructField("exception_percent_total", FloatType(), nullable=False),
        StructField("exception_percent_nonmissing", FloatType(), nullable=False),
        StructField("created_at_ts", TimestampType(), nullable=False),
        StructField("modified_at_ts", TimestampType(), nullable=False),
    ]
)
