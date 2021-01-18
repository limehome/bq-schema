from typing import Iterator, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery.schema import _STRUCT_TYPES, LEGACY_TO_STANDARD_TYPES
from google.cloud.bigquery_v2 import types


# pylint: disable=no-member
def check_schemas(
    local_schema: List[bigquery.SchemaField],
    remote_schema: List[bigquery.SchemaField],
    is_nested: Optional[bool] = False,
) -> Iterator[str]:
    remote_columns = {column.name: column for column in remote_schema}

    for column in local_schema:

        if column.name not in remote_columns:
            yield f"Nested: New column {column}" if is_nested else f"New column {column}"
            continue
        modes_not_equal = column.mode != remote_columns[column.name].mode
        column_field_type = LEGACY_TO_STANDARD_TYPES.get(
            column.field_type, types.StandardSqlDataType.TypeKind.TYPE_KIND_UNSPECIFIED
        )
        remote_column_field_type = LEGACY_TO_STANDARD_TYPES.get(
            remote_columns[column.name].field_type,
            types.StandardSqlDataType.TypeKind.TYPE_KIND_UNSPECIFIED,
        )
        if types.StandardSqlDataType.TypeKind.TYPE_KIND_UNSPECIFIED in (
            column_field_type,
            remote_column_field_type,
        ):
            check_schema_msg = (
                f"Unspecified field type in {column} or {remote_columns[column.name]}"
            )
            yield check_schema_msg
            continue
        field_types_not_equal = column_field_type != remote_column_field_type
        if modes_not_equal or field_types_not_equal:
            check_schema_msg = (
                f"Nested: There is difference between {column} and {remote_columns[column.name]}"
                if is_nested
                else f"There is difference between {column} and {remote_columns[column.name]}"
            )
            yield check_schema_msg
            continue

        if column.field_type in _STRUCT_TYPES:
            yield from check_schemas(
                column.fields, remote_columns[column.name].fields, True
            )
