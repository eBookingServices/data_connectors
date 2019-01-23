from sys import path, stdout
import logging
from pandas import DataFrame

logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Test(object):
    """
    This class is mainly intended as a wrapper around the tests defined for each
    script. It parses the test description and carries the verification of the
    results. Its main method probably is :meth parse_test_result.
    """
    def __init__(self, test_definition):
        """
        Instantiator of the Test class.
        :param test_definition: JSON/dict object containing all the required
        definitions for the test.
        """
        self.test_definition = test_definition
        self.test_description = self.test_definition["description"]
        self.test_query = self.test_definition["query"]
        self.test_setup = self.test_definition["exp_result"]
        self.test_type = self.test_setup["type"]
        logger.info("Instantiated test " + self.test_description)

    def parse_test_result(self, result: DataFrame) -> None:
        """
        Parses the test results and raises an Exception when the latter fails.
        The method consists mainly of mapping the `string` name of the test we
        want to carry and the method to validate the test results.
        :param result: pandas DataFrame containing the test results.
        :return: None
        """
        if self.test_type == "max":
            self.max_test(result)

    def max_test(self, result: DataFrame) -> bool:
        """
        This method defines the test named `max`. It consists in computing the
        maximum value of a given field in the result DataFrame and comparing it
        to expected value defined in the test JSON.
        :param result: Pandas DataFRame containing the test results.
        :return: boolean if the test result passed otherwise raises a
        ValueEreror exception.
        """
        field = self.test_setup["field"]
        max_value = self.test_setup["value"]

        if self.test_setup["allow_nan"] and result[field].isna().sum() > 0:
            logger.error("Failed test MAX value."
                         "Found NaN values while this is not allowed")
            raise ValueError("Test 'Max' failed")

        if result[field].max() != max_value:
            logger.error("Failed test MAX value."
                         "Got value {}, expected {}".format(max(result),
                                                            max_value))
            raise ValueError("Test 'Max' failed")
        else:
            logger.info("  -> Test passed")
            return True

