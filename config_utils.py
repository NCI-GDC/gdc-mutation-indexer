from enum import Enum


class ReadWriteMode(Enum):
    """
    We can 1) read from saved input file,
           2) write to saved input file,
           3) or neither.
    It doesn't make sense to read from input file x
    and then write that same x, so we exclude both as an option.

    TODO: this should go into BaseConfig. Would need to resolve
    circular dependency (BaseConfig should not depend on low-level
    es-utils).
    """
    neither = 0
    read = 1
    write = 2
