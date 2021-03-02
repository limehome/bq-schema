from typing import Dict, Iterator, List, Optional, Union

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery.schema import _STRUCT_TYPES, LEGACY_TO_STANDARD_TYPES
from google.cloud.bigquery.table import Table
from google.cloud.bigquery_v2 import types

from bq_schema.migration.models import ExistingTable, MissingTable
from bq_schema.migration.table_finder import find_tables

_SchemaDiffs = Dict[str, Union[MissingTable, ExistingTable]]

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


def find_schema_differences(
    module_path: str,
    bigquery_client: BigQueryClient,
    global_project: Optional[str],
    global_dataset: Optional[str],
) -> _SchemaDiffs:
    schema_diffs: _SchemaDiffs = {}
    for local_table in find_tables(module_path):
        project = global_project or local_table.project
        assert project, "Project has not been set."
        dataset = global_dataset or local_table.dataset
        assert dataset, "Dataset has not been set."

        table_identifier = f"{project}.{dataset}.{local_table.full_table_name()}"

        try:
            remote_table = bigquery_client.get_table(table_identifier)
            if list(
                check_schemas(local_table.get_schema_fields(), remote_table.schema)
            ):
                schema_diffs[table_identifier] = ExistingTable(
                    local_table=local_table,
                    remote_table=remote_table,
                    schema_diffs=list(
                        check_schemas(
                            local_table.get_schema_fields(), remote_table.schema
                        )
                    ),
                )
        except NotFound:
            schema_diffs[table_identifier] = MissingTable(local_table=local_table)

    return schema_diffs


def print_format_schema_differences(schema_diffs: _SchemaDiffs) -> Dict[str, str]:
    formated_prints = {}
    for table_identifier, difference in schema_diffs.items():
        if isinstance(difference, MissingTable):
            formated_prints[table_identifier] = "Table does not exist in bq"
        elif isinstance(difference, ExistingTable):
            formated_prints[
                table_identifier
            ] = f"Schema differences: {difference.schema_diffs}"

    return formated_prints


def confirm_apply_schema_differences() -> bool:
    valid = {"yes": True, "y": True, "no": False, "n": False}
    while True:
        confirm_apply = (
            input("Do you really want to apply the changes? [Y/N]: ").lower().strip()
        )
        if confirm_apply in valid:
            return valid[confirm_apply]

        print("Possible choices: [Y, y, Yes, yes, N, n, No, no]")


def apply_schema_differences(
    schema_diffs: _SchemaDiffs,
    bigquery_client: BigQueryClient,
) -> None:
    print("Applying changes...")
    for table_identifier, difference in schema_diffs.items():
        if isinstance(difference, MissingTable):
            print("Creating table...")
            table = Table(
                table_identifier,
                schema=difference.local_table.get_schema_fields(),
            )
            if difference.local_table.time_partitioning:
                table.time_partitioning = difference.local_table.time_partitioning
            remote_table = bigquery_client.create_table(table)
            print(remote_table)
        elif isinstance(difference, ExistingTable):
            difference.remote_table.schema = difference.local_table.get_schema_fields()
            print(bigquery_client.update_table(difference.remote_table, ["schema"]))
