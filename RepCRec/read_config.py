"""
Authors:
Saksham Bassi
Aayush Agrawal
"""
import configparser


def read_config(configdir: str):
    """ Reads the configuration file

    Args:
        configdir (str): file path of configuration file

    Returns:
        config: object of ConfigParser with config values

    """
    config = configparser.ConfigParser()
    config.read(configdir)
    return config
