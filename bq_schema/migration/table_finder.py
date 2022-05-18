import importlib
import inspect
import pkgutil
import sys
from typing import Iterator, Optional, Set

from bq_schema.bigquery_table import BigqueryTable


def find_tables(module_path: str) -> Set[BigqueryTable]:
    tables = {}
    for table in _tables_iterator(module_path):
        tables[(table.project, table.dataset, table.name)] = table

    return set(tables.values())


def _tables_iterator(
    root_path: str, current_path: Optional[str] = None
) -> Iterator[BigqueryTable]:
    """
    Recursively find all defined bigquery tables.
    """
    if current_path is None:
        module_path = root_path
    else:
        module_path = current_path[current_path.find(root_path) :]

    sys_path = sys.path[0].replace("/", ".")

    module_path_to_iterate = module_path
    module_path = module_path.replace("/", ".")
    for (_, module_name, is_pkg) in pkgutil.iter_modules([module_path_to_iterate]):
        module_path = module_path.replace(sys_path, "")
        module_path = module_path[1:] if module_path.startswith(".") else module_path
        module = importlib.import_module(f"{module_path}.{module_name}")
        if is_pkg and module.__file__:
            sub_path = module.__file__.replace("/__init__.py", "")
            yield from _tables_iterator(root_path, sub_path)

        for attribute_name in dir(module):
            if attribute_name.startswith("__"):
                continue

            attribute = getattr(module, attribute_name)
            if (
                inspect.isclass(attribute)
                and issubclass(attribute, BigqueryTable)
                and attribute != BigqueryTable
            ):
                yield attribute()
