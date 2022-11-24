import os

from read_config import read_config

path = os.path.dirname(os.path.realpath(__file__))
configdir = "/".join([path, "..", "config.ini"])
config = read_config(configdir)
