import argparse
import logging
import logging.handlers as handlers
import os


def get_args():
    """The function allows to get argparse arguments

    :return: Argparse arguments
    :rtype: argparse
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_one', type=str, required=True)
    parser.add_argument('--file_two', type=str, required=True)
    parser.add_argument('--column_name', type=str, required=False, default='country')
    parser.add_argument('--filter_values', nargs='*',  required=True)
    args = parser.parse_args()
    return args


def get_logger(config):
    """Create a logging object with rotating policy

    :param config: A yaml file allowing to store some parameters
    :type config: yaml
    :return: logging object
    :rtype: Logging
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


def check_files(file_one: str, file_two: str):
    """The function checks if paths to the file exist and if the file are in .csv format

    :param file_one: A path to the dataset_one file
    :type file_one: str
    :param file_two: A path to the dataset_two file
    :type file_two: str
    :return: True or false depending on if the files exist and it they csv format
    :rtype: boolean
    """
    if os.path.exists(file_one) and os.path.exists(file_two):
        if file_one.endswith('csv') and file_two.endswith('csv'):
            return True
    else:
        return False
