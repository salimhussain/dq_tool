from datetime import datetime
from typing import List

from schemas.data_model.dq_validations_schema import DQ_VALIDATIONS_SCHEMA

import logging

logger = logging.getLogger(__name__)


class RuleValidation:
    """Validation class, defines the columns that are required for the validation"""

    __slots__ = [
        "exceptions",
        "validation_key",
        "rule_key",
        "execution_key",
        "data",
    ]

    def __init__(
        self, result: dict, validation_key: int, rule_key: str, execution_key: int
    ) -> None:

        self.exceptions: List[dict] = []
        self.validation_key: int = validation_key
        self.execution_key: int = execution_key
        self.rule_key: int = rule_key

        # Default dictionary for the validation data
        self.data = self._to_dict(result, validation_key, rule_key, execution_key)

    def add_exceptions(self, exceptions: List[dict]) -> None:
        """Add exceptions to the validation"""
        self.exceptions.extend(exceptions)

    def to_dict(self) -> dict:
        """Return the validation data"""
        return self.data

    def update(self, *args, **kwargs) -> None:
        """Update the validation data.
        Can pass a list of dictionaries or a dictionary with the data to be updated

        :param args: List of dictionaries with the data to be updated
        :param kwargs: Dictionary with the data to be updated
        """
        for arg in args:
            if isinstance(arg, dict):
                self.data.update(arg)
        self.data.update(kwargs)

    def _to_dict(
        self, result: dict, validation_key: int, rule_key: int, execution_key: int
    ) -> dict:
        """Parse the validation result and return a dictionary with the validation data
        :param result: The result of the validation
        """
        now = datetime.now()
        # Update the validation data with the result information
        validation = {
            "validation_key": validation_key,
            "rule_key": rule_key,
            "execution_key": execution_key,
            "success_flag": result["success"],
            "element_count": result["result"].get("element_count", 0),
            "exception_count": result["result"].get("unexpected_count", 0),
            "exception_percent": result["result"].get("unexpected_percent", 0),
            "exception_percent_nonmissing": result["result"].get("unexpected_percent_nonmissing", 0),
            "exception_percent_total": result["result"].get("unexpected_percent_total", 0),
            "observed_value": result["result"].get("observed_value", None),
            "missing_count": result["result"].get("missing_count", 0),
            "missing_percent": result["result"].get("missing_percent", 0),
            "created_at_ts": now,
            "modified_at_ts": now,
        }
        return validation
