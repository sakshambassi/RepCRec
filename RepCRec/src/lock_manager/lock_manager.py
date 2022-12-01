from typing import Set

from src.enums import AcquireLockPermission, LockType
from src.lock_manager import Lock


class LockManager:
    def __init__(self):
        self.table = dict()  # { variable: int, lock: Lock }

    def acquire_lock(
        self, transaction_id: int, variable: int, lock_type: LockType
    ) -> None:
        """
        A transaction acquires a lock of type "lock_type" on a variable.

        So when a transaction acquires a lock, its id is added against the variable in a
        table data structure.

        Args:
            transaction (int):
            variable (int):
            lock_type (LockType):
        """
        lock = self.table.get(variable, Lock(lock_type, set()))
        lock.transactions.add(transaction_id)
        self.table[variable] = lock

    def can_acquire_write_lock(
        self, variable: int, transaction_id: int
    ) -> AcquireLockPermission:
        """
        - Allow if `variable` is not locked by any transaction.
        - Allow if `variable` is already locked currently by the requesting transaction.
        - Allow if requesting `transaction` was the only one locking the `variable` by a
          READ lock (like a lock promotion)
        - Not allowed otherwise.

        Args:
            variable (int):
            transaction (int):

        Return:
            Return a permission level when write lock is requested.
        """
        if variable not in self.table or len(self.table[variable].transactions) == 0:
            return AcquireLockPermission.ALLOWED

        lock = self.table[variable]
        if (
            lock.type == LockType.READ
            and transaction_id in lock.transactions
            and len(lock.transactions) == 1
        ):
            return AcquireLockPermission.ALLOWED_IF_EMPTY_WAIT_QUEUE

        if lock.type == LockType.WRITE and transaction_id in lock.transactions:
            return AcquireLockPermission.ALLOWED

        return AcquireLockPermission.NOT_ALLOWED

    def can_acquire_read_lock(
        self, variable: int, transaction_id: int
    ) -> AcquireLockPermission:
        """
        - Allow if `variable` is not locked by any transaction.
        - Allow if `variable` is locked by a READ lock (requested by any transaction).
        - Allow if `variable` is already locked currently by the requesting transaction
          (even if WRITE lock).
        - Not allowed otherwise.

        Args:
            variable (int):
            transaction (int):

        Return:
            Return a permission level when read lock is requested.
        """
        if variable not in self.table or len(self.table[variable].transactions) == 0:
            return AcquireLockPermission.ALLOWED

        lock = self.table[variable]

        if lock.type == LockType.READ:
            return AcquireLockPermission.ALLOWED_IF_EMPTY_WAIT_QUEUE

        if lock.type == LockType.WRITE and transaction_id in lock.transactions:
            return AcquireLockPermission.ALLOWED_TRANSACTION

        return AcquireLockPermission.NOT_ALLOWED

    def get_lock_type(self, variable: int) -> LockType:
        """

        Args:
            variable ():

        Returns:

        """
        pass

    def get_all_transaction_locks(self, variable: int) -> Set[int]:
        """
        Args:
            variable (int)

        Returns:
            Set of transaction_ids that have requested a lock on the `variable`
        """
        if variable in self.table:
            return self.table.get(variable).transactions
        return set()

    def release_all_locks(self):
        """
        Lock table is cleared/reinitialized when locks are to be released.
        """
        self.table = dict()

    def release_lock(self, variable: int) -> bool:
        """

        Args:
            variable (int):

        Returns:

        """
        pass

    def release_transaction_lock(self, transaction_id: int):
        """
        The lock table is iterated over all the keys (variables) and the requested
        transaction is removed from the set against it when released.

        Args:
            transaction_id (int)

        Returns:
            Set of transaction_ids that have requested a lock on the `variable`
        """
        for _, lock in self.table.items():
            if transaction_id in lock.transactions:
                lock.transactions.remove(transaction_id)
