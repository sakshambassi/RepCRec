import os

from read_config import read_config
from src.transaction_manager import TransactionManager


def main():
    path = os.path.dirname(os.path.realpath(__file__))
    configdir = "/".join([path, "config.ini"])

    config = read_config(configdir)
    transaction_manager = TransactionManager(
        total_sites=int(config["CONSTANTS"]["num_sites"])
    )

    # TODO: complete prepare_input and follow the order of events
    transaction_manager.prepare_input(filename="tests/inputs/input8")


if __name__ == "__main__":
    main()
