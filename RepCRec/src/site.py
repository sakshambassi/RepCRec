import os

from src.enums import LockType
from src.lock_manager import LockManager
from read_config import read_config
import math
from typing import Dict

path = os.path.dirname(os.path.realpath(__file__))
configdir = '/'.join([path, '..', 'config.ini'])
config = read_config(configdir)

COUNT_VARIABLES = int(config['CONSTANTS']['num_variables'])


class Site:
    def __init__(self, id_: int):
        self.id = id_
        self.active = True
        self.lock_manager = LockManager()
        self.data = dict()
        self.stale = dict()
        self.cache = dict()

    def acquire_lock(self, transaction: int, variable: int, locktype: LockType) -> bool:
        """

        Returns:

        """
        pass

    # TODO: remove comment
    # def recover

    def activate(self):
        """

        Returns:

        """
        self.active = True

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
        for i in range(1, COUNT_VARIABLES + 1):
            if i % 2 == 0:
                self.stale[i] = False
            self.data[i] = i * 10

    def _floor_of_tick(self, data_per_variable: Dict[int, int], tick: int) -> int:
        closeness = math.inf
        closest_tick = None
        for current_tick, _ in data_per_variable.items():
            if current_tick <= tick and tick - closest_tick < closeness:
                closeness = tick - current_tick
                closest_tick = current_tick
        return closest_tick

    def get_value(self, variable: int, tick: int) -> int:
        """

        Returns:

        """
        data_so_far = self.data[variable]
        # find a key in `data_so_far` that is less than equal to tick
        closest_tick = self._floor_of_tick(data_so_far, tick)
        return data_so_far[closest_tick]

    def get_last_committed_time(self, variable: int, tick: int) -> int:
        """

        Returns:

        """
        data_so_far = self.data[variable]
        return self._floor_of_tick(data_so_far, tick)

    def dump(self):
        """

        Returns:

        """
        pass

    # TODO: remove comment
    # def isUp def is_up

    def get_status(self) -> bool:
        """

        Returns:

        """
        return self.active

    def release_all_locks(self):
        """

        Returns:

        """
        self.lock_manager.release_all_locks()

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

    def fail(self):
        """

        Returns:

        """
        self.active = False
        for i in range(1, COUNT_VARIABLES+1):
            if i % 2 == 0:
                self.stale[i] = True
        self.cache = dict()

    def is_stale(self, variable: int) -> bool:
        """

        Returns:

        """
        return self.stale.get(variable)

    def set_cache(self, variable: int, value: int, tick: int):
        """

        Returns:

        """
        cache_so_far = self.cache.get(variable, dict())
        cache_so_far[tick] = value
        self.cache[variable] = cache_so_far

    def is_variable_unique(self, variable: int):
        """

        Returns:

        """
        return variable in self.data and variable % 2 != 0

    def is_variable_present(self, variable: int):
        """

        Returns:

        """
        return variable in self.data
