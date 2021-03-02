import argparse
from argparse import Namespace
from pprint import pprint
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud.bigquery.table import Table

from bq_schema.cli.bigquery_connection import create_connection
from bq_schema.migration.schema_diff import (
    apply_schema_differences,
    confirm_apply_schema_differences,
    find_schema_differences,
    print_format_schema_differences,
)
from bq_schema.migration.table_finder import find_tables


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="Manage your bigquery table migrations."
    )
    parser.add_argument("--project", required=False, help="Target bigquery project.")
    parser.add_argument("--dataset", required=False, help="Target bigquery dataset")
    parser.add_argument(
        "--module-path", required=True, help="Module path to your bigquery models."
    )

    parser.add_argument(
        "--apply",
        default=False,
        action="store_true",
        help="If set to true, this script will apply all open migrations.",
    )

    parser.add_argument(
        "--validate",
        default=False,
        action="store_true",
        help="If set to true, this script will fail if a difference in schemas is found. Useful for CI.",
    )

    return parser.parse_args()


def main(
    project: Optional[str],
    dataset: Optional[str],
    module_path: str,
    apply: bool,
    validate: bool,
) -> None:
    bigquery_client = create_connection()
    global_project = project
    global_dataset = dataset
    schemas_errors = {}

    print("Finding schema differences...")
    schema_diffs = find_schema_differences(
        module_path=module_path,
        bigquery_client=bigquery_client,
        global_project=project,
        global_dataset=dataset,
    )
    formated_schema_diff = print_format_schema_differences(schema_diffs=schema_diffs)
    if formated_schema_diff:
        print("Schema Differences:")
        pprint(formated_schema_diff)
        if validate:
            raise Exception(formated_schema_diff)
    else:
        print("No schema differences found.")

    if apply:
        if confirm_apply_schema_differences():
            apply_schema_differences(
                schema_diffs=schema_diffs, bigquery_client=bigquery_client
            )


def cli() -> None:
    args = parse_args()
    main(args.project, args.dataset, args.module_path, args.apply, args.validate)


if __name__ == "__main__":
    cli()
