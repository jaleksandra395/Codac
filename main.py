import argparse
from df_class import DataFrameCreator
import logging
import logging.handlers as handlers
import os
from pyspark.sql import SparkSession
import yaml


def get_args():
    """_summary_

    :return: _description_
    :rtype: _type_
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_one', type=str, required=True)
    parser.add_argument('--file_two', type=str, required=True)
    parser.add_argument('--countries', nargs='+', help='delimited list input', required=True)
    # for string with spaces use three double quotes '''string with space'''
    args = parser.parse_args()
    return args


def get_logger(config):
    """_summary_

    :param config: _description_
    :type config: _type_
    :return: _description_
    :rtype: _type_
    """
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

    return logger


def check_paths(file_one, file_two):
    """_summary_

    :param file_one: _description_
    :type file_one: _type_
    :param file_two: _description_
    :type file_two: _type_
    :return: _description_
    :rtype: _type_
    """
    if os.path.exists(file_one) and os.path.exists(file_two):
        if file_one.endswith('csv') and file_two.endswith('csv'):
            return True
    else:
        return False


def main():
    """_summary_
    """
    args = get_args()

    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    logger = get_logger(config)
        
    spark_session = SparkSession.builder.appName("assignment").getOrCreate()
    
    if check_paths(args.file_one, args.file_two):
        clients_df = DataFrameCreator(args.file_one, spark_session, logger)
        clients_df.filter_country_column(args.countries, logger)
        clients_df.drop_columns(config["drop_names"], logger)

        cards_df = DataFrameCreator(args.file_two, spark_session, logger)
        cards_df.drop_columns(config["drop_names"], logger)

        clients_df.join_dfs(cards_df, config["join_on"], logger)
        clients_df.rename_column(config["rename_names"], logger)
        clients_df.save_to_file(config["output_path"], logger)
    else:
        logger.info("The paths to files do not exist.")

    logger.info("The program has stopped running.")


if __name__ == '__main__':
    main()
     