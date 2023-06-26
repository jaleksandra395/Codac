from logging import Logger, getLogger
from pyspark.sql import SparkSession
from pytest import fixture


@fixture(name="spark_session", scope="session")
def spark_session_fixture() -> SparkSession:
    """_summary_

    :return: _description_
    :rtype: SparkSession
    """
    spark_session = SparkSession.builder.appName("test_session").getOrCreate()
    yield spark_session
    spark_session.stop()


@fixture(name="spark_logger", scope="session")
def logger_fixture() -> Logger:
    """_summary_

    :return: _description_
    :rtype: Logger
    """
    logger = getLogger()
    return logger
