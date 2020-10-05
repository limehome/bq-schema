import inspect
from datetime import date, datetime, time
from decimal import Decimal

from google.cloud.bigquery import SchemaField

from bq_schema_placeholder.schema_converter import schema_to_dataclass
from bq_schema_placeholder.types.type_mapping import Timestamp


class NestedSchema:
    int_field: int


class Schema:
    string_field: str
    bytes_field: bytes
    int_field: int
    float_field: float
    numeric_field: Decimal
    bool_field: bool
    timestamp_field: Timestamp
    date_field: date
    time_field: time
    datetime_field: datetime
    nested_field: NestedSchema


def test_schema_to_dataclass():
    schema = [
        SchemaField("string_field", "STRING", "REQUIRED", None, ()),
        SchemaField("bytes_field", "BYTES", "REQUIRED", None, ()),
        SchemaField("int_field", "INT64", "REQUIRED", None, ()),
        SchemaField("float_field", "FLOAT64", "REQUIRED", None, ()),
        SchemaField("numeric_field", "NUMERIC", "REQUIRED", None, ()),
        SchemaField("bool_field", "BOOL", "REQUIRED", None, ()),
        SchemaField("timestamp_field", "TIMESTAMP", "REQUIRED", None, ()),
        SchemaField("date_field", "DATE", "REQUIRED", None, ()),
        SchemaField("time_field", "TIME", "REQUIRED", None, ()),
        SchemaField("datetime_field", "DATETIME", "REQUIRED", None, ()),
        SchemaField("nested_field", "STRUCT", "REQUIRED", None, ()),
    ]
    expected = f"@dataclass\n{inspect.getsource(Schema)}"
    assert schema_to_dataclass("Schema", schema) == expected.strip()
