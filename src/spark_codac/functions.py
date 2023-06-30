import argparse
import logging
import logging.handlers as handlers
import os
from typing import Tuple


def get_args() -> argparse.Namespace:
    """The function allows to get argparse arguments

    :return: Argparse arguments
    :rtype: argparse
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_one', type=str, required=True, help='Path to file one')
    parser.add_argument('--file_two', type=str, required=True, help='Path to file two')
    parser.add_argument('--column_name', type=str, required=False, default='country', help='Column name by which the dataset should be filtered, by default country')
    parser.add_argument('--countries', nargs='*',  required=False, default=['Netherlands', 'United Kingdom'], help='Values to filter')
    args = parser.parse_args()
    return args


def get_logger(config) -> logging.Logger:
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


def check_files(file_one: str, file_two: str) -> bool:
    """The function checks if paths to the file exist and if the file are in .csv format

    :param file_one: A path to the dataset_one file
    :type file_one: str
    :param file_two: A path to the dataset_two file
    :type file_two: str
    :return: True or false depending on if the files exist
    :rtype: boolean
    """
    return os.path.exists(file_one) and os.path.exists(file_two)
