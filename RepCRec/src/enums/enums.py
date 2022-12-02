"""
Authors: 
Saksham Bassi
Aayush Agrawal
"""
from enum import Enum


class AcquireLockPermission(Enum):
    ALLOWED = 0
    NOT_ALLOWED = 1
    ALLOWED_IF_EMPTY_WAIT_QUEUE = 2
    ALLOWED_TRANSACTION = 3


class InstructionType(Enum):
    FAIL = 0
    RECOVER = 1
    DUMP = 2
    BEGINRO = 3
    BEGIN = 4
    END = 5
    NO = 6


class LockType(Enum):
    WRITE = 0
    READ = 1
    NO = 2


class TransactionType(Enum):
    WRITE = 0
    READ = 1
    READONLY = 2
    NONE = 3
