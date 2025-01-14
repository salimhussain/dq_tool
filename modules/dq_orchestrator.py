from modules.rule_manager import RuleManager
from modules.gx_context_manager import GXContextManager
from modules.validation_manager import ValidationManager
from modules.data_model_manager import DataModelManager

from typing import List

import logging

logger = logging.getLogger(__name__)


class DQOrchestrator:

    def __init__(self, rule_tables: List[str]):
        """Class to orchestrate the data quality workflow.

        :param rule_manager: The rule manager.
        :param context_manager: The context manager.
        :param validation_manager: The validation manager.
        """
        self.rule_manager = RuleManager()
        self.context_manager = GXContextManager()
        self.validation_manager = ValidationManager()

        self.run_workflow(rule_tables)

    def run_workflow(self, rules_tables: List[str]) -> None:
        """Run the data quality workflow for the specified rules tables list.

        :param rules_tables: List of table names containing rules.
        """
        # Load the rules from the specified table
        rules_df = self.rule_manager.update_rules(rules_tables)

        # Iterate over the rules and update the context
        self.rule_manager.parse_rules(rules_df, self.context_manager)

        # Run the rules
        self.validation_manager.run(self.context_manager)

        # Save the results
        self.context_manager.save_results()
