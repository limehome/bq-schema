from dataclasses import dataclass

from bq_schema.bigquery_table import BigqueryTable


@dataclass
class FirstTableSchema:
    a_column: int


class FirstTable(BigqueryTable):
    name = "first_table"
    schema = FirstTableSchema
