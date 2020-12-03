import os
import pathlib

from bq_schema.migration.table_finder import find_tables


def test_table_finder():
    file_path = pathlib.Path(__file__).parent
    tables_dir = os.path.join(file_path, "tables")
    table_names = {t.name for t in find_tables(tables_dir)}
    expected_table_names = {"first_table", "second_table"}
    assert expected_table_names == table_names
