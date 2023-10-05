from dataclasses import dataclass
from typing import List

from google.cloud.bigquery.table import Table

from bq_schema.bigquery_table import BigqueryTable


@dataclass
class MissingTable:
    local_table: BigqueryTable


@dataclass
class ExistingTable:
    local_table: BigqueryTable
    remote_table: Table
    schema_diffs: List[str]
