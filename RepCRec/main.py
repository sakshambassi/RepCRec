import os

from read_config import read_config
from TransactionManager import transactionmanager


def main():
    path = os.path.dirname(os.path.realpath(__file__))
    configdir = '/'.join([path, 'config.ini'])

    config = read_config(configdir)
    transaction_manager = transactionmanager.TransactionManager(
        total_sites=config['CONSTANTS']['num_sites'])

    # TODO: complete prepare_input and follow the order of events
    transaction_manager.prepare_input(None)


if __name__ == "__main__":
    main()
