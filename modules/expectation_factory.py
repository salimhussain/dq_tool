from modules.expectation import *


class ExpectationFactory:

    # Map GX_EXPECTATION values to specific classes
    EXPECTATION_MAP = {
        "ExpectColumnValuesToNotBeNull": ExpectColumnValuesToNotBeNull,
        "UnexpectedRowsExpectation": UnexpectedRowsExpectation,
        "ExpectColumnValuesToMatchRegex": ColumnExpectation,
        "ExpectColumnValuesToNotMatchRegex": ColumnExpectation,
        "ExpectColumnValuesToMatchLikePattern": ColumnExpectation,
        "ExpectColumnValuesToBeBetween": ColumnExpectation,
        "ExpectColumnPairValuesAToBeGreaterThanB": ColumnExpectation,
        "ExpectColumnValuesToBeInSet": ColumnExpectation,
        "ExpectColumnValuesToNotBeInSet": ColumnExpectation,
        "ExpectColumnValuesToBeUnique": ColumnExpectation,
        "ExpectCompoundColumnsToBeUnique": ColumnExpectation,
    }

    @classmethod
    def create_expectation(cls, data: dict):
        expectation_type = data["gx_expectation"]
        expectation_class = cls.EXPECTATION_MAP.get(expectation_type)

        if not expectation_class:
            raise ValueError(f"Unknown GX_EXPECTATION type: {expectation_type}")
        
        print(f"\t- Creating expectation of type: {expectation_type}")
        
        try:
            # Return an instance of the appropriate class
            return expectation_class(data)
        except Exception as e:
            print(f"Error creating expectation: {e}")
            return None
