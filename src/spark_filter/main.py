from src.spark_filter.df_class import DataFrameCreator
from src.spark_filter.functions import check_files, get_args, get_logger
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
        clients_df = DataFrameCreator(args.file_one, spark_session, logger)
        clients_df.filter_column(args.filter_values, logger, args.column_name)
        clients_df.drop_columns(config["drop_names"], logger)

        cards_df = DataFrameCreator(args.file_two, spark_session, logger)
        cards_df.drop_columns(config["drop_names"], logger)

        clients_df.join_dfs(cards_df, config["join_on"], logger)
        clients_df.rename_column(config["rename_names"], logger)
        clients_df.save_to_file(config["output_path"], logger)
    else:
        logger.info("The files do not exist.")

    logger.info("The program has stopped running.")


if __name__ == '__main__':
    main()
     