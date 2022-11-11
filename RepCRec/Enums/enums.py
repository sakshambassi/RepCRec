from enum import Enum


class LockType(Enum):
    WRITE = 0
    READ = 1

class Instruction(Enum):
    FAIL = 0
    RECOVER = 1
    DUMP = 2
    BEGINRO = 3
    BEGIN = 4
    END = 5