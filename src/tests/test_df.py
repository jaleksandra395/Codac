import chispa
from logging import Logger
from pytest import fixture 
from pyspark.sql import SparkSession, DataFrame
from src.spark_filter.df_class import DataFrameCreator


@fixture(name="fixture_source_df")
def fixture_source_df(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2', 'col3', 'col4', 'col5', 'col6'])
    return df


@fixture(name="expected_filtered_col4")
def expected_filtered_col4(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2', 'col3', 'col4', 'col5', 'col6'])
    return df


@fixture(name="expected_filter_column_not_in_df")
def expected_filter_column_not_in_df(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2', 'col3', 'col4', 'col5', 'col6'])
    return df


@fixture(name="expected_rename_no_column_provided")
def expected_rename_no_column_provided(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2', 'col3', 'col4', 'col5', 'col6'])
    return df


@fixture(name="expected_rename_column_not_in_df")
def expected_rename_column_not_in_df(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2', 'col3', 'col4', 'col5', 'col6'])
    return df

@fixture(name="expected_one_column_renamed_df")
def expected_one_column_renamed_df(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1', 'col2_2', 'col3', 'col4', 'col5', 'col6'])
    return df


@fixture(name="expected_all_columns_renamed_df")
def expected_all_columns_renamed_df(spark_session: SparkSession) -> DataFrame:
    df_data = [('col1val1', 'col2val1', 'col3val1', 'col4val1', 'col5val1', 'col6val1'),
            ('col1val2', 'col2val2', 'col3val2', 'col4val2', 'col5val2', 'col6val2'),
            ('col1val3', 'col2val3', 'col3val3', 'col4val3', 'col5val3', 'col6val3'),
            ('col1val4', 'col2val4', 'col3val4', 'col4val4', 'col5val4', 'col6val4'),
            ('col1val5', 'col2val5', 'col3val5', 'col4val5', 'col5val5', 'col6val5')]
    df = spark_session.createDataFrame(df_data, ['col1_1', 'col2_2', 'col3_3', 'col4_4', 'col5_5', 'col6_6'])
    return df


# TEST FILTER
def test_filter_one_column_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_filtered_col4: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.filter_column(["col4val2"], spark_logger, "col4")
    chispa.assert_df_equality(test_df.df, expected_filtered_col4, ignore_row_order=True)


def test_filter_column_not_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_filter_column_not_in_df: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.filter_column(["col4val2"], spark_logger, "col7")
    chispa.assert_df_equality(test_df.df, expected_filter_column_not_in_df, ignore_row_order=True)


# TEST RENAME
def test_rename_no_column_provided(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_rename_no_column_provided: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.rename_column({}, spark_logger)
    chispa.assert_df_equality(test_df.df, expected_rename_no_column_provided, ignore_row_order=True)


def test_rename_column_not_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_rename_column_not_in_df: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.rename_column({"col7":"col8"}, spark_logger)
    chispa.assert_df_equality(test_df.df, expected_rename_column_not_in_df, ignore_row_order=True)


def test_rename_one_column_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_one_column_renamed_df: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.rename_column({"col2":"col2_2"}, spark_logger)
    chispa.assert_df_equality(test_df.df, expected_one_column_renamed_df, ignore_row_order=True)


def test_rename_all_columns_in_df(spark_session: SparkSession, spark_logger: Logger, fixture_source_df: DataFrame, expected_all_columns_renamed_df: DataFrame) -> None:
    test_df = DataFrameCreator(None, spark_session, spark_logger, None, fixture_source_df)
    test_df.rename_column({"col1":"col1_1", "col2":"col2_2", "col3":"col3_3", "col4":"col4_4", "col5":"col5_5", "col6":"col6_6"}, spark_logger)
    chispa.assert_df_equality(test_df.df, expected_all_columns_renamed_df, ignore_row_order=True)
