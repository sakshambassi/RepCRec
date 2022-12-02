"""
Authors:
Saksham Bassi
Aayush Agrawal
"""
import os

from read_config import read_config

path = os.path.dirname(os.path.realpath(__file__))
configdir = "/".join([path, "..", "config.ini"])
config = read_config(configdir)


def log(message, end=None):
    """ prints on output screen

    Args:
        message (): message to be printed
        end (str): end for print

    Returns:
        None
    """
    if end is None:
        print(message)
    else:
        print(message, end=end)
