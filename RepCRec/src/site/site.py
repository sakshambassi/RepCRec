"""
Authors:
Saksham Bassi
Aayush Agrawal
"""
import math
from typing import Dict, Set

from src.enums import LockType, AcquireLockPermission, LockType
from src.lock_manager import LockManager
from src.utils import config, log

COUNT_VARIABLES = int(config["CONSTANTS"]["num_variables"])


class Site:
    def __init__(self, id_: int):
        self.id = id_
        self.active = True
        self.lock_manager = LockManager()
        self.data = dict()  # {variable: {time: value}}
        self.stale = dict()
        self.cache = dict()  # {variable: {time: value}}

    def acquire_lock(self, transaction_id: int, variable: int, locktype: LockType):
        """
        In the site, transaction `transaction_id` acquires a lock of type `locktype` on
        the variable `variable`.
        """
        self.lock_manager.acquire_lock(transaction_id, variable, locktype)

    def activate(self):
        """
        Activate the site.
        """
        self.active = True

    def can_acquire_read_lock(
            self, variable: int, transaction_id: int
    ) -> AcquireLockPermission:
        """
            Returns (AcquireLockPermission): Permission type that the transaction can
            get when trying to acquire a READ lock on the given variable.

        """
        return self.lock_manager.can_acquire_read_lock(variable, transaction_id)

    def can_acquire_write_lock(
            self, variable: int, transaction_id: int
    ) -> AcquireLockPermission:
        """
            Returns (AcquireLockPermission): Permission type that the transaction can
            get when trying to acquire a WRITE lock on the given variable.
        """
        return self.lock_manager.can_acquire_write_lock(variable, transaction_id)

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
                    and timestamp - current_timestamp < closeness
            ):
                closeness = timestamp - current_timestamp
                closest_timestamp = current_timestamp
        return closest_timestamp

    def get_value(self, variable: int, timestamp: int) -> int:
        """ Get last committed value for the give variable on/before the time
        `timestamp`.

        Args:
            variable (int): variable value
            timestamp (int): timestamp tick value

        Returns:
            int
        """
        data_so_far = self.data[variable]
        # find a key in `data_so_far` that is less than equal to timestamp
        closest_timestamp = self._floor_of_timestamp(data_so_far, timestamp)
        return data_so_far[closest_timestamp]

    def commit_cache(self, variable: int) -> None:
        """
        Variable in the site is committed (as part of a transaction), so the cache for
        the variable is passed to the "data" attribute and cache is cleaned.

        Args:
            variable (int): variable value
        """
        data_for_variable = self.data.get(variable, {})
        cache_for_variable = self.cache.get(variable, {})
        for time, value in cache_for_variable.items():
            data_for_variable[time] = value
            self.stale[variable] = False
        self.data[variable] = data_for_variable
        if variable in self.cache:
            self.log_commit(variable, self.cache[variable])
        self.cache[variable] = {}

    def log_commit(self, variable: int, committed_data: Dict[int, int]):
        """ logs commit message on output screen

        Args:
            variable (int): variable value
            committed_data (dict): commit details

        Returns:
            None
        """
        for timestamp, value in committed_data.items():
            log(f"Committed variable {variable}; with value {value}; on site {self.id}; at time {timestamp}")

    def get_last_committed_time(self, variable: int, timestamp: int) -> int:
        """ fetches kast commited timestamp

        Args:
            variable (int): variable value
            timestamp (int): tick timestamp

        Returns:
            int: Last committed time for variable before the time `timestamp`
        """
        data_so_far = self.data[variable]
        return self._floor_of_timestamp(data_so_far, timestamp)

    def get_all_transaction_locks(self, variable: int) -> Set[int]:
        """
        Returns: All the transactions that have a lock on the given variable in the site.

        """
        return self.lock_manager.get_all_transaction_locks(variable)

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
                value = data_so_far[self._floor_of_timestamp(data_so_far, timestamp)]
            log(f"x{variable}:{value}", end=" ")
        log("")

    def is_active(self) -> bool:
        """
        The site is active or not (failed)

        Returns:
            bool: site's active status
        """
        return self.active

    def release_all_locks(self):
        """

        Returns:

        """
        self.lock_manager.release_all_locks()

    def release_all_transaction_locks(self, transaction_id: int):
        """ release all given transaction's locks

        Args:
            transaction_id (int): id of transaction of which transaction locks need to be released
        """
        self.lock_manager.release_transaction_lock(transaction_id)

    def shutdown(self) -> None:
        """ When a site is shutdown, its active attribute is set to False

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
        """ When a site has failed/shutdown, even variables in it are marked stale. This is
        because they can go out of sync with the copies of even variables on other
        (active) sites.

        This function returns if a variable in the site is marked stale.

        Args:
            variable (int)

        Returns: bool
        """
        return self.stale.get(variable)

    def set_cache(self, variable: int, value: int, timestamp: int):
        """ In the current site, for the given variable, store the value against the
        timestamp. When the variable in the site is committed (as part of a transaction),
        the cache is passed to the "data" attribute and cache should be cleaned.

        Returns:

        """
        cache_so_far = self.cache.get(variable, dict())
        cache_so_far[timestamp] = value
        self.cache[variable] = cache_so_far

    def is_variable_unique(self, variable: int) -> bool:
        """ Even variables are present on all sites and odd variables are present on only
        one site as per specification. So only odd variables can be unique.

        Args:
            variable (int)

        Returns:
            bool: whether variable is unique(odd) or not
        """
        return variable in self.data and variable % 2 != 0

    def is_variable_present(self, variable: int) -> bool:
        """ checks if variable is present or not

        Args:
            variable (int): variable value

        Returns:
            bool: whether variable is present in current sites's data
        """
        return variable in self.data
