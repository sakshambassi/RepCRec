from typing import Set

from src.enums import LockType


class Lock:
    def __init__(self, type: LockType, transactions: Set[int]):
        self.type = type
        self.transactions = transactions
