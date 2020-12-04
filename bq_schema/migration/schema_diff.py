from typing import Iterator, List

from google.cloud import bigquery


def find_new_columns(
    local_schema: List[bigquery.SchemaField],
    remote_schema: List[bigquery.SchemaField],
) -> Iterator[bigquery.SchemaField]:
    remote_columns = {column.name: column for column in remote_schema}

    for column in local_schema:
        if column.name not in remote_columns:
            yield column
            continue

        if column.field_type == "RECORD":
            yield from find_new_columns(
                column.fields, remote_columns[column.name].fields
            )
