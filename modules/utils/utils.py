import importlib

from datetime import datetime


# Custom function to process and convert 'today' to a datetime object
# Can be updated with more cases, just add more conditions
def custom_decoder(obj: dict) -> dict:
    """Custom decoder to handle 'today' string and convert it to datetime object

    :param obj: dictionary object to be processed
    :return: dictionary with the 'today' string converted to datetime object
    """
    return {
        key: (datetime.today() if value == "today" else value)
        for key, value in obj.items()
    }


# Get an especific Expectation Class from GX Expectation module
def my_import(name: str, arguments: dict) -> object:
    """Import a specific Expectation Class from GX Expectation module

    :param name: name of the Expectation Class to be imported
    :param arguments: dictionary with the arguments to be passed to the Expectation Class
    :return: object with the Expectation Class
    """
    module = importlib.import_module("great_expectations.expectations")
    return getattr(module, name)(**arguments)


def format_type(expectation_type: str) -> str:
    """Format the expectation type string

    :param expectation_type: string with the expectation type to be formatted
    :return: string with the formatted expectation type
    """
    return expectation_type.replace("_", " ").title().replace(" ", "")
