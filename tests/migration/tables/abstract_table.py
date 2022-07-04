from abc import ABC
from dataclasses import dataclass

from bq_schema.bigquery_table import BigqueryTable


@dataclass
class AbstractTableSchema:
    a_column: int


class FirstTable(BigqueryTable, ABC):
    pass


class SecondAbstractTable(FirstTable):
    name = "second_abstract_table"
    schema = AbstractTableSchema
