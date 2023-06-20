import argparse
from df_class import DataFrameCreator
import logging
import logging.handlers as handlers
from pyspark.sql import DataFrame, SparkSession
import yaml


def main():

    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(config["logs_formatter"], config["time_formatter"])

    logHandler = handlers.RotatingFileHandler(config["logs_path"], maxBytes=868*3, backupCount=3)
    logHandler.setLevel(logging.INFO)
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
        

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_one', type=str, required=True)
    parser.add_argument('--file_two', type=str, required=True)
    parser.add_argument('--countries', nargs='+', help='delimited list input', required=True)
    # for string with spaces use three double quotes '''string with space'''
    args = parser.parse_args()
    
    spark_session = SparkSession.builder.appName("assignment").getOrCreate()
    df1 = DataFrameCreator(args.file_one, spark_session, logger)
    df1.filter_country_column(args.countries, logger)
    df1.drop_columns(config["drop_names"], logger)

    df2 = DataFrameCreator(args.file_two, spark_session, logger)
    df2.drop_columns(config["drop_names"], logger)

    df1.join_dfs(df2, config["join_on"], logger)
    df1.rename_column(config["rename_names"], logger)
    df1.save_to_file(config["output_path"], logger)

    logger.info("The program has finished SUCCESSFULLY.")



if __name__ == '__main__':
    main()
    
    