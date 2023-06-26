from functools import reduce
from logging import Logger
from pyspark.sql import DataFrame, SparkSession
from typing import List, Dict


class DataFrameCreator:
    """The class represents a DataFrame
    """
    def __init__(self, file_path: str, spark_session: SparkSession, logger: Logger, df: DataFrame = None) -> None:
        """The method reads a csv file and creates a DataFrame

        :param file_path: A path which leads to a csv file
        :type file_path: str
        :param spark_session: A spark session object
        :type spark_session: SparkSession
        :param logger: A logger object
        :type logger: Logger
        """
        if df:
            self.df = df 
            logger.info("Because any path has been provided a DataFrame object from the provided DataFrame has been created.")
        else:
            self.spark_session = spark_session
            self.file_path = file_path
            self.logger = logger
            self.df = spark_session.read.option("header", True).csv(file_path)
            logger.info(f"The dataset from {self.file_path.split('/')[-1]} has been read SUCCESSFULLY.")
        

    def filter_country_column(self, filter_values: List, logger: Logger, column: str) -> DataFrame:
        """The method filters a country column in DataFrame by a list of countries
        :param filter_values: A list of values by which a DataFrame has to be filtered
        :type filter_values: List
        :param logger: A logger object
        :type logger: Logger
        :param column: A column where are values by which the DataFrame has to be filtered
        :type column: str
        :return: A filtered DataFrame
        :rtype: DataFrame
        """
        self.column = column
        self.filter_values = filter_values
        self.df = self.df.filter(self.df[column].isin(filter_values))
        self.logger = logger
        logger.info(f"The dataset has been filtered by countries SUCCESSFULLY.")
        return self.df


    def drop_columns(self, columns_to_drop: List, logger: Logger) -> DataFrame:
        """The method drops unnecessary columns which were listed in columns_to_drop list

        :param columns_to_drop: A list of columns names which have to be dropped
        :type columns_to_drop: List
        :param logger: A logger object
        :type logger: Logger
        :return: A DataFrame without unneccesary columns
        :rtype: DataFrame
        """
        self.columns_to_drop = columns_to_drop 
        self.df = self.df.drop(*columns_to_drop)
        self.logger = logger
        logger.info("The unnecessary columns have been dropped SUCCESSFULLY.") 
        return self.df
    
    
    def join_dfs(self, other, col: str, logger: Logger) -> DataFrame:
        """The method joins two DataFrames objects of the DataFrameCreator class 
            based on the col parameter value.

        :param other: Other object of the DataFrameCreator class
        :type other: DataFrameCreator
        :param col: A name of a column based on which join has to be done
        :type col: str
        :param logger: A logger object
        :type logger: Logger
        :return: A DataFrame after join
        :rtype: DataFrame
        """
        self.col = col
        self.df = self.df.join(other.df, col, "inner")
        self.logger = logger 
        logger.info("The datasets have been joined SUCCESSFULLY.")
        return self.df
    

    def rename_column(self, renames_dict: Dict, logger: Logger) -> DataFrame:
        """The method renames columns based on the renames_dict dictionary 

        :param renames_dict: A dictionary where keys are original column names and values are their substitutes
        :type renames_dict: Dict
        :param logger: A logger object
        :type logger: Logger
        :return: A DataFrame with renamed columns
        :rtype: DataFrame
        """
        self.df = reduce(lambda df, idx: df.withColumnRenamed(list(renames_dict.keys())[idx], 
                                                              list(renames_dict.values())[idx]), 
                                                              range(len(renames_dict)), self.df)
        self.logger = logger
        logger.info("The columns have been renamed SUCCESSFULLY.")
        return self.df
    
    
    def save_to_file(self, output_path: str, logger: Logger) -> DataFrame:
        """The method saves a DataFrame in specific path

        :param output_path: A path for created DataFrame to be saved
        :type output_path: str
        :param logger: A logger object
        :type logger: Logger
        :return: A DataFrame
        :rtype: DataFrame
        """
        self.df.write.option("header", True).mode("overwrite").csv(output_path)
        self.logger = logger
        logger.info("The dataset has been saved SUCCESSFULLY.")
        return self.df
