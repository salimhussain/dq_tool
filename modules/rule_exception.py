from datetime import datetime


class RuleException:
    """Exception class, defines the columns that are required for the exception"""

    @staticmethod
    def get(
        validation_key: int, rule_key: int, execution_key: int, exception_row_pk: str, unexpected_value: str
    ) -> dict:
        """Parse the exception and return a dictionary with the exception data

        :param validation_key: The validation key
        :param rule_key: The rule key
        :param exception_row_pk: The composite key of the exception
        :return: A dictionary with the exception data
        """
        now = datetime.now()

        return {
            "validation_key": validation_key,
            "rule_key": rule_key,
            "execution_key": execution_key,
            "exception_row_pk": exception_row_pk,
            "exception_row_value": unexpected_value,
            "fixed_exception_flag": False,
            "new_exception_flag": False,
            "row_delete_flag": False,
            "created_at_ts": now,
            "modified_at_ts": now,
        }
