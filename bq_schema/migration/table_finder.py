import inspect
import os
from importlib import util as importlib_util
from typing import Iterator, Set

from bq_schema.bigquery_table import BigqueryTable


def find_tables(module_path: str) -> Set[BigqueryTable]:
    tables = {}
    for table in _tables_iterator(module_path):
        tables[(table.project, table.dataset, table.name)] = table

    return set(tables.values())


def _tables_iterator(module_path: str) -> Iterator[BigqueryTable]:
    """
    Recursively find all defined bigquery tables.
    """
    for (dir_path, _, file_names) in os.walk(module_path):
        for file_name in file_names:
            if file_name.endswith(".py"):
                spec = importlib_util.spec_from_file_location(
                    "not_important", os.path.join(dir_path, file_name)
                )
                assert spec is not None
                mod = importlib_util.module_from_spec(spec)

                spec.loader.exec_module(mod)  # type: ignore

                for _, obj in inspect.getmembers(mod, inspect.isclass):
                    if issubclass(obj, BigqueryTable) and not obj == BigqueryTable:
                        instance = obj()
                        yield instance
