from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
)

DQ_EXCEPTIONS_SCHEMA = StructType(
    [
        StructField(
            "validation_key",
            LongType(),
            nullable=False,
            metadata={"description": "Foreign key to the validation"},
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
        ),
        StructField(
            "exception_row_pk",
            StringType(),
            nullable=False,
            metadata={"description": "Primary key of the row that failed the rule"},
        ),
        StructField(
            "exception_row_value",
            StringType(),
            nullable=False,
            metadata={"description": "Value of the row that failed the rule"},
        ),
        StructField(
            "fixed_exception_flag",
            BooleanType(),
            nullable=False,
            metadata={
                "description": "Flag to indicate if the exception has been fixed"
            },
        ),
        StructField(
            "new_exception_flag",
            BooleanType(),
            nullable=False,
            metadata={"description": "Flag to indicate if the exception is new"},
        ),
        StructField(
            "row_delete_flag",
            BooleanType(),
            nullable=False,
            metadata={"description": "Flag to indicate if the row has been deleted"},
        ),
        StructField(
            "created_at_ts",
            TimestampType(),
            nullable=False,
            metadata={"description": "Timestamp when the record was created"},
        ),
        StructField(
            "modified_at_ts",
            TimestampType(),
            nullable=False,
            metadata={"description": "Timestamp when the record was last modified"},
        ),
    ]
)
