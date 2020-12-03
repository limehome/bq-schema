from google.cloud.bigquery import SchemaField
from bq_schema.migration.schema_diff import find_new_columns


def test_find_new_columns():
    local_schema = [SchemaField("a", "INTEGER")]
    assert list(find_new_columns(local_schema, [])) == local_schema


def test_find_new_columns_missing_record():
    local_schema = [SchemaField("a", "RECORD")]
    assert list(find_new_columns(local_schema, [])) == local_schema


def test_find_new_columns_missing_column_in_record():
    local_schema = [SchemaField("a", "RECORD", fields=[SchemaField("b", "INTEGER")])]
    remote_schema = [SchemaField("a", "RECORD", fields=[])]
    assert list(find_new_columns(local_schema, remote_schema)) == [
        SchemaField("b", "INTEGER", "NULLABLE", None, ())
    ]
