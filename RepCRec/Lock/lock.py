from Enums import enums


class Lock:
    def __init__(self):
        self.table = dict()

    def acquire_lock(self, transaction: int, variable: int, lock_type: enums.LockType) -> bool:
        """ Acquires lock

        Args:
            transaction (int):
            variable (int):
            lock_type (LockType):

        Returns:
            None
        """
        pass

    def get_lock_type(self, variable: int) -> enums.LockType:
        """

        Args:
            variable ():

        Returns:

        """
        pass

    def release_lock(self, variable: int) -> bool:
        """

        Args:
            variable (int):

        Returns:

        """
        pass
