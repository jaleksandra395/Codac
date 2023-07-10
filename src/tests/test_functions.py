import argparse
from src.spark_codac.functions import get_args


def test_get_arguments_from_argparse(mocker):
    mocker.patch('argparse.ArgumentParser.parse_args',
                 return_value=argparse.Namespace(file_one='one_data.csv', file_two='two_data.csv', countries=['France', 'United States']))

    args = get_args()
    args_result = args.file_one, args.file_two, args.countries
    expected_result = ('one_data.csv', 'two_data.csv', ['France', 'United States'])
    assert args_result == expected_result
