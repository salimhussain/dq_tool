from modules.expectation_factory import ExpectationFactory
from modules.data_model_manager import DataModelManager
from modules.expectation import BaseExpectation

from datetime import date
from typing import List

import logging

logger = logging.getLogger(__name__)


class ExpectationManager:

    __slots__ = [
        "expectations",
    ]

    def __init__(self):
        self.expectations: dict = {}

    def _rule_enabled(self, rule) -> bool:
        """Checks if a rule should be executed

        :param rule: Rule data
        :return: True if the rule should be executed, False otherwise
        """
        today = date.today()
        return rule["rule_enabled"] == "Y" and (
            rule["start_date"] <= today <= rule["expiry_date"]
        )

    def get_expectation(self, rule) -> tuple:
        """Get an expectation object for a given rule
        
        :param rule: Rule data
        :return: Expectation object
        """
        print(f"\t- Checking if rule {rule['rule_key']} is enabled:")
        enabled = self._rule_enabled(rule)
        print(f"\t- Rule {rule['rule_key']} is enabled: {enabled}")

        expectation = None
        
        if not enabled:
            print(f"\t- Skipping expectation {rule['rule_key']} for table {rule['table_name']}")
        else:
            expectation = ExpectationFactory.create_expectation(rule)
        return expectation, enabled

    def add_expectation(self, expectation) -> None:
        print(f"\t- Adding expectation {expectation.gx_expectation_name} {expectation.rule_key} to ExpectationManager")
        self.expectations[expectation.gx_expectation.id] = expectation
        
    def save_expectation_suite(self):
        data_model_manager = DataModelManager()
        data_model_manager.save_expectation_suite(self.expectations.values())

    def parse_results(
        self,
        results: List[dict],
        element_count: int,
        start_validation_key: int,
        execution_key: int,
    ):
        validation_key = start_validation_key
        for result in results:
            validation_key += 1
            expectation = self.expectations.get(result.expectation_config["id"])
            expectation.execution_key = execution_key
            expectation.parse_validation(
                result, element_count, validation_key, execution_key
            )
