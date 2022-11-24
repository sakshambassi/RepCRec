from enums import LockType
from typing import Set


class Lock:
    def __init__(self, type: LockType, transactions: Set[int]):
        self.type = type
        self.transactions = transactions
