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

DQ_RULE_SCHEMA = StructType(
    [
        StructField(
            "integration_id",
            StringType(),
            nullable=False,
            metadata={
                "primary_key": True,
                "description": "Combination of table_name~column~rule_no~stream_name",
            },
        ),
        StructField(
            "rule_key",
            LongType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "description": "Unique identifier for the rule. Auto-incremented",
            },
        ),
        StructField(
            "rule_no",
            IntegerType(),
            nullable=False,
            metadata={"description": "Unique identifier for a rule within a file"},
        ),
        StructField(
            "rule_description",
            StringType(),
            nullable=False,
            metadata={"description": "Description of the rule"},
        ),
        StructField(
            "system_evaluated",
            StringType(),
            nullable=False,
            metadata={"description": "System evaluated"},
        ),
        StructField(
            "rule_priority",
            StringType(),
            nullable=False,
            metadata={"description": "Priority of the rule (P1, P2 or P3)"},
        ),
        StructField(
            "start_date",
            DateType(),
            nullable=False,
            metadata={"description": "Start date of the rule"},
        ),
        StructField(
            "expiry_date",
            DateType(),
            nullable=False,
            metadata={"description": "Expiry date of the rule"},
        ),
        StructField(
            "rule_enabled",
            StringType(),
            nullable=False,
            metadata={
                "description": "Defines if the rule is enabled or disabled (Y/N)"
            },
        ),
        StructField(
            "data_domain",
            StringType(),
            nullable=False,
            metadata={"description": "Data domain"},
        ),
        StructField(
            "data_product",
            StringType(),
            nullable=False,
            metadata={"description": "Data product"},
        ),
        StructField(
            "data_owner",
            StringType(),
            nullable=False,
            metadata={"description": "Data owner"},
        ),
        StructField(
            "business_steward",
            StringType(),
            nullable=False,
            metadata={"description": "Business steward"},
        ),
        StructField(
            "it_steward",
            StringType(),
            nullable=True,
            metadata={"description": "IT steward"},
        ),
        StructField(
            "threshold",
            FloatType(),
            nullable=False,
            metadata={
                "description": "Decimal value for threshold. Defines the percentage of rows that can not fail the rule to be considered successful"
            },
        ),
        StructField(
            "weight",
            FloatType(),
            nullable=False,
            metadata={
                "description": "Decimal value for weight. Defines the importance of the rule"
            },
        ),
        StructField(
            "rule_type_key",
            IntegerType(),
            nullable=False,
            metadata={"description": "Foreign key to the rule type"},
        ),
        StructField(
            "catalog",
            StringType(),
            nullable=False,
            metadata={"description": "Catalog name"},
        ),
        StructField(
            "schema",
            StringType(),
            nullable=False,
            metadata={"description": "Schema name"},
        ),
        StructField(
            "table_name",
            StringType(),
            nullable=False,
            metadata={"description": "Table name"},
        ),
        StructField(
            "column",
            StringType(),
            nullable=False,
            metadata={"description": "Column name"},
        ),
        StructField(
            "table_key",
            StringType(),
            nullable=False,
            metadata={
                "description": "Unique identifier for the table that the rule is applied to. Comma-separated of column names in case of composite key"
            },
        ),
        StructField(
            "gx_arguments",
            StringType(),
            nullable=False,
            metadata={"description": "Arguments for the GX function"},
        ),
        StructField(
            "gx_conditions",
            StringType(),
            nullable=True,
            metadata={"description": "Conditions for the GX function"},
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
