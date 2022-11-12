import os

from Enums import enums
from Lock import lock
from read_config import read_config

path = os.path.dirname(os.path.realpath(__file__))
configdir = '/'.join([path, '..', 'config.ini'])
config = read_config(configdir)


class Site:
    def __init__(self, id_: int):
        self.id = id_
        self.active = True
        self.data = dict()
        self.lock = lock.Lock()

    def acquire_lock(self, transaction: int, variable: int, locktype: enums.LockType) -> bool:
        """

        Returns:

        """
        pass

    def activate(self):
        """

        Returns:

        """
        pass

    def check_variable_exists(self, variable: int) -> bool:
        """

        Args:
            variable ():

        Returns:

        """
        pass

    def initialize(self):
        """ Initializes the site object with data

        Returns:
            None
        """
        for i in range(1, int(config['CONSTANTS']['num_variables']) + 1):
            self.data[i] = i * 10

    def dump(self):
        """

        Returns:

        """
        pass

    def get_status(self) -> bool:
        """

        Returns:

        """
        pass

    def release_lock(self, variable: int) -> bool:
        """

        Args:
            variable ():

        Returns:

        """
        pass

    def shutdown(self):
        """

        Returns:

        """
        pass
