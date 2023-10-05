import argparse
from argparse import Namespace

from bq_schema.cli.bigquery_connection import create_connection
from bq_schema.schema_converter import schema_to_dataclass


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a bigquery table schema into a dataclass."
    )
    parser.add_argument("--project", required=True, help="Target bigquery project.")
    parser.add_argument("--dataset", required=True, help="Target bigquery dataset")
    parser.add_argument(
        "--table-name", required=True, help="Target bigquery table_name"
    )

    return parser.parse_args()


def main(project: str, dataset: str, table_name: str) -> None:
    client = create_connection()
    table = client.get_table(f"{project}.{dataset}.{table_name}")
    print("from dataclasses import dataclass")
    print(schema_to_dataclass(table_name, table.schema))


def cli() -> None:
    args = parse_args()
    main(args.project, args.dataset, args.table_name)


if __name__ == "__main__":
    cli()
