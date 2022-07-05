import os
import pathlib

import pytest

from bq_schema.migration.table_finder import find_tables


def test_table_finder():
    file_path = pathlib.Path(__file__).parent
    tables_dir = os.path.join(file_path, "tables")
    table_names = {t.name for t in find_tables(tables_dir, True)}
    expected_table_names = {"first_table", "second_table", "second_abstract_table"}
    assert expected_table_names == table_names


def test_table_finder_fail_on_missing_implementation():
    with pytest.raises(NotImplementedError):
        file_path = pathlib.Path(__file__).parent
        tables_dir = os.path.join(file_path, "tables")
        _ = {t.name for t in find_tables(tables_dir, False)}


def test_table_schema_fields():
    file_path = pathlib.Path(__file__).parent
    tables_dir = os.path.join(file_path, "tables")
    for local_table in find_tables(tables_dir, True):
        schema_fields = local_table.get_schema_fields()
        assert len(schema_fields) > 0
