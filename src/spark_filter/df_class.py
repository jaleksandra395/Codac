from functools import reduce
from logging import Logger
from pyspark.sql import DataFrame, SparkSession
from typing import List, Dict


class DataFrameRepresent:
    """The class represents a DataFrame
    """
    def __init__(self, spark_session: SparkSession, logger: Logger) -> None:
        """The method gets spark session and logger

        :param spark_session: A SparkSession object
        :type spark_session: SparkSession
        :param logger: A logger object
        :type logger: Logger
        """
        self.logger = logger
        self.spark_session = spark_session
    
    def build_df_from_path(self, file_path: str) -> DataFrame:
        """The method reads data from file path and creates DataFrame

        :param: file_path: A path to a file containing data
        :type: file_path: str
        :return: A created DataFrame
        :rtype: DataFrame
        """
        self.file_path = file_path
        file_format = file_path.split('.')[-1]
        self.df = self.spark_session.read.format(file_format).option("header", True).load(file_path)
        self.logger.info(f"The dataset from {self.file_path.split('/')[-1]} has been read SUCCESSFULLY.")
        return self.df
    
    def build_df_from_df(self, df: DataFrame) -> DataFrame:
        """The method creates DataFrame from a DataFrame object

        :param df: A DataFrame object
        :type df: DataFrame
        :return: A created DataFrame
        :rtype: DataFrame
        """
        self.df = df 
        self.logger.info("A DataFrame object from the provided DataFrame has been created.")
        return self.df

    def filter_column(self, filter_values: List, column: str) -> DataFrame:
        """The method filters a column in DataFrame by a list of values
        :param filter_values: A list of values by which a DataFrame has to be filtered
        :type filter_values: List
        :param column: A column where are values by which the DataFrame has to be filtered
        :type column: str
        :return: A filtered DataFrame
        :rtype: DataFrame
        """
        self.column = column
        self.filter_values = filter_values
        if column in self.df.columns:
            self.df = self.df.filter(self.df[column].isin(filter_values))
            self.logger.info(f"The dataset has been filtered by {column} SUCCESSFULLY.")
        else:
            self.logger.warning(f"{column} does not exist in DataFrame")
        return self.df


    def drop_columns(self, columns_to_drop: List) -> DataFrame:
        """The method drops unnecessary columns which were listed in columns_to_drop list

        :param columns_to_drop: A list of columns names which have to be dropped
        :type columns_to_drop: List
        :return: A DataFrame without unneccesary columns
        :rtype: DataFrame
        """
        self.columns_to_drop = columns_to_drop 
        self.df = self.df.drop(*columns_to_drop)
        self.logger.info("The unnecessary columns have been dropped SUCCESSFULLY.") 
        return self.df
    
    
    def join_dfs(self, other, col: str) -> DataFrame:
        """The method joins two DataFrames objects of the DataFrameCreator class 
            based on the col parameter value.

        :param other: Other object of the DataFrameCreator class
        :type other: DataFrameCreator
        :param col: A name of a column based on which join has to be done
        :type col: str
        :return: A DataFrame after join
        :rtype: DataFrame
        """
        self.col = col
        self.df = self.df.join(other.df, col, "inner")
        self.logger.info("The datasets have been joined SUCCESSFULLY.")
        return self.df
    

    def rename_column(self, renames_dict: Dict) -> DataFrame:
        """The method renames columns based on the renames_dict dictionary.  

        :param renames_dict: A dictionary where keys are original column names and values are their substitutes
        :type renames_dict: Dict
        :return: A DataFrame with renamed columns
        :rtype: DataFrame
        """
        for col_to_rename in list(renames_dict.keys()):
            if col_to_rename not in self.df.columns:
                renames_dict.pop(col_to_rename)

        self.df = reduce(lambda df, idx: df.withColumnRenamed(list(renames_dict.keys())[idx], 
                                                              list(renames_dict.values())[idx]), 
                                                              range(len(renames_dict)), self.df)
        self.logger.info("The columns have been renamed SUCCESSFULLY.")
        return self.df
    
    
    def save_to_file(self, output_path: str) -> DataFrame:
        """The method saves a DataFrame in specific path

        :param output_path: A path for created DataFrame to be saved
        :type output_path: str
        :return: A DataFrame
        :rtype: DataFrame
        """
        self.df.write.option("header", True).mode("overwrite").csv(output_path)
        self.logger.info("The dataset has been saved SUCCESSFULLY.")
        return self.df
