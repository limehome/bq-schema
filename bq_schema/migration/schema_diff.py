from typing import Iterator, List, Optional
from enum import Enum
from google.cloud import bigquery

from bq_schema.types.big_query_field_modes import BigQueryFieldModes
from bq_schema.types.big_query_types import BigQueryTypes


def check_schemas(
    local_schema: List[bigquery.SchemaField],
    remote_schema: List[bigquery.SchemaField],
    is_nested: Optional[bool] = False
) -> Iterator[str]:
    remote_columns = {column.name: column for column in remote_schema}

    for column in local_schema:

        if column.name not in remote_columns:
            yield f"Nested: New column {column}" if is_nested else f"New column {column}"
            continue

        modes_not_equal = column.mode != remote_columns[column.name].mode
        field_types_not_equal = column.field_type != remote_columns[column.name].field_type
        if modes_not_equal or field_types_not_equal:
            check_schema_msg = (
                f"Nested: There is difference between {column} and {remote_columns[column.name]}"
                if is_nested
                else f"There is difference between {column} and {remote_columns[column.name]}"
            )
            yield check_schema_msg
            continue

        if column.field_type == "RECORD":
            yield from check_schemas(
                column.fields, remote_columns[column.name].fields, True
            )
