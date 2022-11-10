import configparser
import os

from TransactionManager import transactionmanager


def read_config():
    """

    Returns:
        config: object of ConfigParser with config values

    """
    config = configparser.ConfigParser()
    path = os.path.dirname(os.path.realpath(__file__))
    configdir = '/'.join([path, 'config.ini'])
    config.read(configdir)
    return config


def main():
    config = read_config()
    transaction_manager = transactionmanager.TransactionManager(
        total_sites=config['CONSTANTS']['sites'])

    transaction_manager.prepare_input(None)


if __name__ == "__main__":
    main()
