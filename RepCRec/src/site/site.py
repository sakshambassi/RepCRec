import math
from typing import Dict, Set

from src.enums import LockType
from src.lock_manager import LockManager
from src.utils import config

COUNT_VARIABLES = int(config["CONSTANTS"]["num_variables"])


class Site:
    def __init__(self, id_: int):
        self.id = id_
        self.active = True
        self.lock_manager = LockManager()
        self.data = dict()  # {variable: {time: value}}
        self.stale = dict()
        self.cache = dict()  # {variable: {time: value}}

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

    def initialize(self) -> None:
        """
        Initializes the site object with data ie { variable: { time, value } }.
        Even variables are present in all sites as per specification.
        """
        for i in range(1, COUNT_VARIABLES + 1):
            if i % 2 == 0 or 1 + (i % 10) == self.id:
                self.stale[i] = False
                self.data[i] = {0: i * 10}

    def _floor_of_timestamp(
        self, data_per_variable: Dict[int, int], timestamp: int
    ) -> int:
        closeness = math.inf
        closest_timestamp = -1
        for current_timestamp, _ in data_per_variable.items():
            if (
                current_timestamp < timestamp
                and timestamp - closest_timestamp < closeness
            ):
                closeness = timestamp - current_timestamp
                closest_timestamp = current_timestamp
        return closest_timestamp

    def get_value(self, variable: int, timestamp: int) -> int:
        """

        Returns:

        """
        data_so_far = self.data[variable]
        # find a key in `data_so_far` that is less than equal to timestamp
        closest_timestamp = self._floor_of_timestamp(data_so_far, timestamp)
        return data_so_far[closest_timestamp]

    # TODO: cache should be cleaned, cant find in ref
    def commit_cache(self, variable: int) -> None:
        """
        Variable in the site is committed (as part of a transaction), so the cache for
        the variable is passed to the "data" attribute and cache is cleaned.

        Args:
            variable (int)
        """
        data_for_variable = self.data.get(variable, {})
        cache_for_variable = self.cache.get(variable, {})
        for time, value in cache_for_variable.items():
            data_for_variable[time] = value
            self.stale[variable] = False
        self.data[variable] = data_for_variable
        self.cache[variable] = {}

    def get_last_committed_time(self, variable: int, timestamp: int) -> int:
        """

        Returns:

        """
        data_so_far = self.data[variable]
        return self._floor_of_timestamp(data_so_far, timestamp)

    def get_all_transaction_locks(self, variable: int) -> Set[int]:
        """

        Returns:

        """
        return self.lock_manager.get_all_transaction_locks(variable)

    # TODO: Print
    def dump(self, timestamp: int) -> None:
        """
        Access the values of all variables in the site at/floor timestamp.

        Args:
            timestamp (int)
        """
        for variable, data_so_far in self.data.items():
            if timestamp in data_so_far:
                value = data_so_far[timestamp]
            else:
                value = data_so_far[self._floor_of_timestamp(
                    data_so_far, timestamp)]

    # TODO: remove comment
    # def isUp def is_up
    def is_active(self) -> bool:
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

    def shutdown(self) -> None:
        """
        When a site is shutdown, its active attribute is set to False

        As per the specification, even variables are present at all the sites.
        So the variable in site should be marked stale to differentiate from
        even variables at other sites that are active.
        """
        self.active = False
        for i in range(1, COUNT_VARIABLES + 1):
            if i % 2 == 0:
                self.stale[i] = True
        self.cache = dict()

    def is_stale(self, variable: int) -> bool:
        """
        When a site has failed/shutdown, even variables in it are marked stale. This is
        because they can go out of sync with the copies of even variables on other
        (active) sites.

        This function returns if a variable in the site is marked stale.

        Args:
            variable (int)

        Returns: bool
        """
        return self.stale.get(variable)

    def set_cache(self, variable: int, value: int, timestamp: int):
        """
        In the current site, for the given variable, store the value against the
        timestamp. When the variable in the site is committed (as part of a transaction),
        the cache is passed to the "data" attribute and cache should be cleaned.

        Returns:

        """
        cache_so_far = self.cache.get(variable, dict())
        cache_so_far[timestamp] = value
        self.cache[variable] = cache_so_far

    def is_variable_unique(self, variable: int) -> bool:
        """
        Even variables are present on all sites and odd variables are present on only
        one site as per specification. So only odd variables can be unique.

        Args:
            variable (int)

        Returns: bool
        """
        return variable in self.data and variable % 2 != 0

    def is_variable_present(self, variable: int) -> bool:
        """
        Args:
            variable (int)

        Returns: bool
        """
        return variable in self.data
