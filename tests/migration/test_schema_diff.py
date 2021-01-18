from google.cloud.bigquery import SchemaField

from bq_schema.migration.schema_diff import check_schemas


def test_check_schemas():
    local_schema = [
        SchemaField("a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER")])
    ]
    remote_schema = [
        SchemaField("a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER")])
    ]
    assert list(check_schemas(local_schema, remote_schema)) == []


def test_find_new_columns():
    local_schema = [SchemaField("a", "INTEGER")]
    assert list(check_schemas(local_schema, [])) == [
        "New column SchemaField('a', 'INTEGER', 'NULLABLE', None, ())"
    ]


def test_find_new_columns_missing_record():
    local_schema = [SchemaField("a", "RECORD")]
    assert list(check_schemas(local_schema, [])) == [
        "New column SchemaField('a', 'RECORD', 'NULLABLE', None, ())"
    ]


def test_find_new_columns_missing_column_in_record():
    local_schema = [SchemaField("a", "RECORD", fields=[SchemaField("b", "INTEGER")])]
    remote_schema = [SchemaField("a", "RECORD", fields=[])]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "Nested: New column SchemaField('b', 'INTEGER', 'NULLABLE', None, ())"
    ]


def test_check_mode_fail():
    local_schema = [
        SchemaField("a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER")])
    ]
    remote_schema = [
        SchemaField("a", "RECORD", "NULLABLE", fields=[SchemaField("b", "INTEGER")])
    ]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "There is difference between SchemaField('a', 'RECORD', 'REQUIRED', "
        "None, (SchemaField('b', 'INTEGER', 'NULLABLE', None, ()),)) and "
        "SchemaField('a', 'RECORD', 'NULLABLE', None, (SchemaField('b', 'INTEGER', "
        "'NULLABLE', None, ()),))",
    ]


def test_check_mode_fail_nested():
    local_schema = [
        SchemaField(
            "a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER", "NULLABLE")]
        )
    ]
    remote_schema = [
        SchemaField(
            "a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER", "REQUIRED")]
        )
    ]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "Nested: There is difference between SchemaField('b', 'INTEGER', 'NULLABLE', "
        "None, ()) and SchemaField('b', 'INTEGER', 'REQUIRED', None, ())",
    ]


def test_check_field_type_fail():
    local_schema = [SchemaField("a", "STRING", "REQUIRED")]
    remote_schema = [SchemaField("a", "INTEGER", "REQUIRED")]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "There is difference between SchemaField('a', 'STRING', 'REQUIRED', None, ()) "
        "and SchemaField('a', 'INTEGER', 'REQUIRED', None, ())",
    ]


def test_check_field_type_fail_nested():
    local_schema = [
        SchemaField(
            "a", "RECORD", "REQUIRED", fields=[SchemaField("b", "STRING", "REQUIRED")]
        )
    ]
    remote_schema = [
        SchemaField(
            "a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER", "REQUIRED")]
        )
    ]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "Nested: There is difference between SchemaField('b', 'STRING', 'REQUIRED', "
        "None, ()) and SchemaField('b', 'INTEGER', 'REQUIRED', None, ())",
    ]


def test_check_field_type_unspecified():
    local_schema = [SchemaField("a", "RANDOM_FIELD_TYPE", "REQUIRED")]
    remote_schema = [SchemaField("a", "INTEGER", "REQUIRED")]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "Unspecified field type in SchemaField('a', 'RANDOM_FIELD_TYPE', 'REQUIRED', None, ()) "
        "or SchemaField('a', 'INTEGER', 'REQUIRED', None, ())"
    ]


def test_check_field_type_unspecified_nested():
    local_schema = [
        SchemaField(
            "a",
            "RECORD",
            "REQUIRED",
            fields=[SchemaField("b", "RANDOM_FIELD_TYPE", "REQUIRED")],
        )
    ]
    remote_schema = [
        SchemaField(
            "a", "RECORD", "REQUIRED", fields=[SchemaField("b", "INTEGER", "REQUIRED")]
        )
    ]
    assert list(check_schemas(local_schema, remote_schema)) == [
        "Unspecified field type in SchemaField('b', 'RANDOM_FIELD_TYPE', 'REQUIRED', "
        "None, ()) or SchemaField('b', 'INTEGER', 'REQUIRED', None, ())",
    ]
