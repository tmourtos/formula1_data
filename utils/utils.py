from datetime import datetime, date, timedelta
from decimal import Decimal
from random import randint
from tabulate import tabulate
from typing import Generator, Union


def exponential_backoff_retries(max_backoff: int = 1200, max_retries: int = 20, max_random_s: int = 300) -> Generator:
    """For a finite number of retries, yield the wait duration

    :param max_backoff: The maximum backoff duration
    :param max_retries: The maximum number of retries
    :param max_random_s: The maximum number of milliseconds to be randomly added
    :return: The waiting time
    """
    maximum_backoff = max_backoff
    retry = 1
    while retry <= max_retries:
        yield min(2 ^ retry + randint(1, max_random_s), maximum_backoff)
        retry += 1

def batch(iterable: list, batch_size=500) -> Generator:
    """Receive an iterable and return specific batch of it

    :param iterable: The iterable
    :param batch_size: Te batch size
    """
    length = len(iterable)
    if length <= batch_size:
        yield iterable
    else:
        for ndx in range(0, length, batch_size):
            yield iterable[ndx:min(ndx + batch_size, length)]


def json_converter(obj: Union[datetime, date, Decimal]) -> str:
    """Create a string representation of an object

    :param obj: The object to be converted to string
    :return: Its string representation
    """
    if type(obj) in [datetime, date, Decimal]:
        return obj.__str__()


def daterange(start_date: date, end_date: date) -> Generator:
    """Date range function. Creates an iterable between 2 dates

    :param start_date: The start date
    :param end_date: The end date
    :return: A date each time
    """
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def unique(_list):
    """
        Keep unique elements of a list
    :param _list: The input list
    :return: The list's unique values
    """
    _set = set(_list)
    return list(_set)


def print_df(dataframe, head_rows=None, tail_rows=None):
    """
        Print a Pandas dataframe
    :param dataframe: The dataframe to print
    :param head_rows: The number of top rows to print
    :param tail_rows: The number of bottom rows to print
    """
    if head_rows:
        print(tabulate(dataframe.head(head_rows), headers='keys', tablefmt='psql'))
    elif tail_rows:
        print(tabulate(dataframe.tail(tail_rows), headers='keys', tablefmt='psql'))
    else:
        print(tabulate(dataframe, headers='keys', tablefmt='psql'))


def round_to_ten(number) -> int:
    """
        Round a number to the nearest decade
    :param number: The number to be rounded
    :return: The number rounded to the nearest decade
    """
    return round(number, -1)

def get_years_between(start_year, end_year):
    years = list()
    for year in range(start_year, end_year + 1):
        years.append(year)
    return years
