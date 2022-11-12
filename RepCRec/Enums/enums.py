from enum import Enum


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

class TransactionType(Enum):
    WRITE = 0
    READ = 1
    READONLY = 2
    NONE = 3