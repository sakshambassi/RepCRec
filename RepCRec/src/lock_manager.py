from src.enums import LockType, AcquireLockPermission


class LockManager:
    def __init__(self):
        # { variable: int, lock: Lock }
        self.table = dict()

    def acquire_lock(
        self, transaction: int, variable: int, lock_type: LockType
    ) -> bool:
        """ Acquires lock

        Args:
            transaction (int):
            variable (int):
            lock_type (LockType):

        Returns:
            None
        """
        pass

    def can_acquire_write_lock(self, variable: int, transaction: int):

        if variable not in self.table or len(self.table[variable].transactions) == 0:
            return AcquireLockPermission.ALLOWED

        lock = self.table[variable]

        if (
            lock.type == LockType.READ
            and transaction in lock.transactions
            and len(lock.transactions) == 1
        ):
            return AcquireLockPermission.ALLOWED_IF_EMPTY_WAIT_QUEUE

        if lock.type == LockType.WRITE and transaction in lock.transactions:
            return AcquireLockPermission.ALLOWED

        return AcquireLockPermission.NOT_ALLOWED

    def can_acquire_read_lock(self, variable: int, transaction: int):

        if variable not in self.table or len(self.table[variable].transactions) == 0:
            return AcquireLockPermission.ALLOWED

        lock = self.table[variable]

        if lock.type == LockType.READ:
            return AcquireLockPermission.ALLOWED_IF_EMPTY_WAIT_QUEUE

        if lock.type == LockType.WRITE and transaction in lock.transactions:
            return AcquireLockPermission.ALLOWED_TRANSACTION

        return AcquireLockPermission.NOT_ALLOWED

    def get_lock_type(self, variable: int) -> LockType:
        """

        Args:
            variable ():

        Returns:

        """
        pass

    def release_all_locks(self):
        """

        Returns:

        """
        self.table = dict()

    def release_lock(self, variable: int) -> bool:
        """

        Args:
            variable (int):

        Returns:

        """
        pass

    def release_transaction_lock(self, transaction: int):

        for variable, lock in self.table.items():
            lock.transactions.remove(transaction)
