import os
import sys
sys.path.append(os.getcwd())
from src.spark_codac.df_class import DataFrameRepresent
from src.spark_codac.functions import check_files, get_args, get_logger
from pyspark.sql import SparkSession
import yaml


def main():
    """The main function of the application.
    """
    args = get_args()

    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    logger = get_logger(config)

    spark_session = SparkSession.builder.appName("assignment").getOrCreate()


    if check_files(args.file_one, args.file_two):
        clients_df = DataFrameRepresent(spark_session, logger)
        clients_df.build_df_from_path(args.file_one)
        clients_df.filter_column(args.countries, args.column_name)
        clients_df.drop_columns(config["drop_names"])

        cards_df = DataFrameRepresent(spark_session, logger)
        cards_df.build_df_from_path(args.file_two)
        cards_df.drop_columns(config["drop_names"])

        clients_df.join_dfs(cards_df, config["join_on"])
        clients_df.rename_column(config["rename_names"])
        clients_df.save_to_file(config["output_path"])
    else:
        logger.critical("The files do not exist.")

    spark_session.stop()

    logger.info("The program has finished running.")


if __name__ == '__main__':
    main()
