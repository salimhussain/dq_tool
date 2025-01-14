# By setting os.environ["GX_ANALYTICS_ENABLED"] = "false" before gx is imported, the posthog call go away.
# This call was raising a SSL error during execution of the rules
# https://github.com/great-expectations/great_expectations/issues/10539

import os

os.environ["GX_ANALYTICS_ENABLED"] = "false"

import great_expectations as gx
import logging

logger = logging.getLogger(__name__)


class GXContextManager:

    def __init__(self) -> None:
        """Class to manage the Great Expectations context"""
        self.data_assets = {}
        self.checkpoints = []
        self.context = gx.get_context()

    def get_or_create(self, _name, create_func, get_func, *args, **kwargs) -> object:
        """Helper function to get or create an object.

        :param _name: name of the object to be created or retrieved
        :param create_func: function to create the object
        :param get_func: function to retrieve the object
        :param *args: arguments to be passed to the creation function
        :param **kwargs: keyword arguments to be passed to the creation function
        :return: the object
        """
        try:
            # Attempt to get the object using the provided `get_func`
            return get_func(_name)
        except Exception:
            # If not found, create it using `create_func`
            return create_func(*args, **kwargs)

    def update_context(self, row) -> None:
        """Helper function to update the context with the data from the row

        :param row: pandas Series with the data to be used to update the context
        """
        catalog = row["catalog"]
        schema = row["schema"]
        table = row["table_name"]

        ds_name = f"ds-{catalog}-{schema}"
        asset_name = f"da-{table}"
        suite_name = f"s-{catalog}-{schema}-{table}"
        batch_def_name = f"bd-{catalog}-{schema}-{table}"
        definition_name = f"vd-{catalog}-{schema}-{table}"
        checkpoint_name = f"checkpoint-{catalog}-{schema}-{table}"

        # Get or create data source
        data_source = self.get_or_create(
            ds_name,
            self.context.data_sources.add_spark,
            lambda name: self.context.data_sources.get(name),
            name=ds_name,
        )
        # Get or create data asset
        data_asset = self.get_or_create(
            asset_name,
            data_source.add_dataframe_asset,
            lambda name: data_source.get_asset(name),
            name=asset_name,
        )
        # Get or create batch definition
        batch_definition = self.get_or_create(
            batch_def_name,
            data_asset.add_batch_definition_whole_dataframe,
            lambda name: data_asset.get_batch_definition(name),
            batch_def_name,
        )
        # Get or create expectation suite
        expectation_suite = self.get_or_create(
            suite_name,
            self.context.suites.add,
            lambda name: self.context.suites.get(name),
            gx.core.expectation_suite.ExpectationSuite(name=suite_name),
        )
        # Get or create validation definition
        validation_definition = self.get_or_create(
            definition_name,
            self.context.validation_definitions.add,
            lambda name: self.context.validation_definitions.get(name),
            gx.core.validation_definition.ValidationDefinition(
                name=definition_name,
                data=batch_definition,
                suite=expectation_suite,
            ),
        )

        # Add checkpoint if it doesn't exist
        if checkpoint_name not in self.checkpoints:
            self.checkpoints.append(checkpoint_name)
            self.context.checkpoints.add(
                gx.Checkpoint(
                    name=checkpoint_name,
                    validation_definitions=[validation_definition],
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [
                            col.strip() for col in row["table_key"].split(",")
                        ],
                        "partial_unexpected_count": 0,
                        "exclude_unexpected_values": False,
                        "include_unexpected_rows": True,
                        "return_unexpected_index_query": True,
                    },
                )
            )

    def save_results(self) -> None:
        """Save the results of the validations"""
        for data_asset in self.data_assets.values():
            print(f"[*] Saving expectation suite for data asset {data_asset.table}")
            data_asset.save_expectation_suite()
            print(f"[*] Expectation suite saved for data asset {data_asset.table}")
