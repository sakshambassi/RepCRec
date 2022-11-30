import argparse
import glob
import os

from read_config import read_config
from src.transaction_manager import TransactionManager
from src.utils import log


def main():
    args = parse_args()
    path = os.path.dirname(os.path.realpath(__file__))
    configdir = "/".join([path, "config.ini"])
    config = read_config(configdir)

    if args.run_all:
        for filename in sorted(glob.glob('tests/inputs/*')):
            run_input(config, filename)
    else:
        filename = f"tests/inputs/input{args.input}"
        run_input(config, filename)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-all', action="store_true",
                        help='Whether to run on all tests')
    parser.add_argument("--input", default=1, type=int,
                        help="Which input test to run")
    args = parser.parse_args()
    return args


def run_input(config, filename: str):
    log(f"{'-'*100}\nProcessing for input file={filename}")
    transaction_manager = TransactionManager(
        total_sites=int(config["CONSTANTS"]["num_sites"])
    )
    transaction_manager.prepare_input(filename=filename)

if __name__ == "__main__":
    main()
