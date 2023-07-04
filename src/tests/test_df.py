import chispa
from logging import Logger
from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from src.spark_codac.df_class import DataFrameRepresent


@fixture
def fixture_source_df(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_filtered_col4(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(4, 5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_filter_column_not_in_df(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_rename_no_column_provided(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_rename_column_not_in_df(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_one_column_renamed_df(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0', 'col1', 'col2_2', 'col3', 'col4', 'col5'])
    return df


@fixture
def expected_all_columns_renamed_df(spark_session: SparkSession) -> DataFrame:
    df_data = [tuple('val' + str(i) + str(j) for i in range(6)) for j in range(5)]
    df = spark_session.createDataFrame(df_data, ['col0_0', 'col1_1', 'col2_2', 'col3_3', 'col4_4', 'col5_5'])
    return df


# TEST FILTER
def test_filter_one_column_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_filtered_col4: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.filter_column(["val44"], "col4")
    chispa.assert_df_equality(test_df.df, expected_filtered_col4, ignore_row_order=True)


def test_filter_column_not_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_filter_column_not_in_df: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.filter_column(["valX"], "col7")
    chispa.assert_df_equality(test_df.df, expected_filter_column_not_in_df, ignore_row_order=True)


# TEST RENAME
def test_rename_no_column_provided(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_rename_no_column_provided: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.rename_column({})
    chispa.assert_df_equality(test_df.df, expected_rename_no_column_provided, ignore_row_order=True)


def test_rename_column_not_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_rename_column_not_in_df: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.rename_column({"col7": "col8"})
    chispa.assert_df_equality(test_df.df, expected_rename_column_not_in_df, ignore_row_order=True)


def test_rename_one_column_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_one_column_renamed_df: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.rename_column({"col2": "col2_2"})
    chispa.assert_df_equality(test_df.df, expected_one_column_renamed_df, ignore_row_order=True)


def test_rename_all_columns_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_all_columns_renamed_df: DataFrame) -> None:
    test_df = DataFrameRepresent(spark_session, spark_logger)
    test_df.build_df_from_df(fixture_source_df)
    test_df.rename_column({"col0": "col0_0", "col1": "col1_1", "col2": "col2_2", "col3": "col3_3", "col4": "col4_4", "col5": "col5_5"})
    chispa.assert_df_equality(test_df.df, expected_all_columns_renamed_df, ignore_row_order=True)
