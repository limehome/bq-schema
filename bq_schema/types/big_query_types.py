from enum import Enum


class BigQueryTypes(str, Enum):
    STRING = "STRING"
    BYTES = "BYTES"
    INTEGER = "INT64"
    INT64 = "INT64"
    FLOAT64 = "FLOAT64"
    FLOAT = "FLOAT64"
    NUMERIC = "NUMERIC"
    BOOL = "BOOL"
    BOOLEAN = "BOOL"
    STRUCT = "STRUCT"
    RECORD = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
