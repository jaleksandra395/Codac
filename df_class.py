from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from typing import List, Dict


class DataFrameCreator:
    def __init__(self, file_path: str, spark_session: SparkSession, logger) -> None:
        self.spark_session = spark_session
        self.file_path = file_path
        self.logger = logger
        self.df = spark_session.read.option("header", True).csv(file_path)
        logger.info("The dataset has been read SUCCESSFULLY.")
        

    def filter_country_column(self, countries: List, logger) -> DataFrame:
        self.countries = countries
        self.df = self.df.filter(self.df.country.isin(countries))
        self.logger = logger
        logger.info("The dataset has been filtered by countries SUCCESSFULLY.")
        return self.df


    def drop_columns(self, columns_to_drop: List, logger) -> DataFrame:
        self.columns_to_drop = columns_to_drop 
        self.df = self.df.drop(*columns_to_drop)
        self.logger = logger
        logger.info("The unnecessary columns have been dropped SUCCESSFULLY.") 
        return self.df
    
    
    def join_dfs(self, other, col: str, logger) -> DataFrame:
        self.col = col
        self.df = self.df.join(other.df, col, "inner")
        self.logger = logger 
        logger.info("The datasets have been joined SUCCESSFULLY.")
        return
    

    def rename_column(self, renames_dict: Dict, logger) -> DataFrame:
        self.df = reduce(lambda df, idx: df.withColumnRenamed(list(renames_dict.keys())[idx], 
                                                              list(renames_dict.values())[idx]), 
                                                              range(len(renames_dict)), self.df)
        self.logger = logger
        logger.info("The columns have been renamed SUCCESSFULLY.")
        return self.df
    
    
    def save_to_file(self, output_path: str, logger) -> DataFrame:
        self.df.write.option("header", True).mode("overwrite").csv(output_path)
        self.logger = logger
        logger.info("The dataset has been saved SUCCESSFULLY.")
        return self.df
