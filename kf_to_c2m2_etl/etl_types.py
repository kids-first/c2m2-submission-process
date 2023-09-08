from enum import Enum


class ETLType(Enum):
    FHIR = 1
    DS = 2

    @classmethod
    def from_string(cls, value):
        for etl_val in cls:
            if value.upper() == etl_val.name:
                return etl_val