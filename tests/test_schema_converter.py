import inspect
from typing import List, Optional
from datetime import date, datetime, time
from decimal import Decimal

from google.cloud.bigquery import SchemaField

from bq_schema_placeholder.schema_converter import schema_to_dataclass
from bq_schema_placeholder.types.type_mapping import Timestamp


class RequiredNestedField:
    int_field: int


class RequiredSchema:
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
    required_nested_field: RequiredNestedField


def test_required_schema_to_dataclass():
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
        SchemaField(
            "required_nested_field",
            "STRUCT",
            "REQUIRED",
            None,
            (SchemaField("int_field", "INT64", "REQUIRED", None, ()),),
        ),
    ]
    expected = f"@dataclass\n{inspect.getsource(RequiredNestedField)}\n@dataclass\n{inspect.getsource(RequiredSchema)}"
    assert schema_to_dataclass("RequiredSchema", schema) == expected.strip()


class NullableNestedField:
    int_field: Optional[int]


class NullableSchema:
    string_field: Optional[str]
    bytes_field: Optional[bytes]
    int_field: Optional[int]
    float_field: Optional[float]
    numeric_field: Optional[Decimal]
    bool_field: Optional[bool]
    timestamp_field: Optional[Timestamp]
    date_field: Optional[date]
    time_field: Optional[time]
    datetime_field: Optional[datetime]
    nullable_nested_field: Optional[NullableNestedField]


def test_optional_schema_to_dataclass():
    schema = [
        SchemaField("string_field", "STRING", "NULLABLE", None, ()),
        SchemaField("bytes_field", "BYTES", "NULLABLE", None, ()),
        SchemaField("int_field", "INT64", "NULLABLE", None, ()),
        SchemaField("float_field", "FLOAT64", "NULLABLE", None, ()),
        SchemaField("numeric_field", "NUMERIC", "NULLABLE", None, ()),
        SchemaField("bool_field", "BOOL", "NULLABLE", None, ()),
        SchemaField("timestamp_field", "TIMESTAMP", "NULLABLE", None, ()),
        SchemaField("date_field", "DATE", "NULLABLE", None, ()),
        SchemaField("time_field", "TIME", "NULLABLE", None, ()),
        SchemaField("datetime_field", "DATETIME", "NULLABLE", None, ()),
        SchemaField(
            "nullable_nested_field",
            "STRUCT",
            "NULLABLE",
            None,
            (SchemaField("int_field", "INT64", "NULLABLE", None, ()),),
        ),
    ]
    expected = f"@dataclass\n{inspect.getsource(NullableNestedField)}\n@dataclass\n{inspect.getsource(NullableSchema)}"
    assert schema_to_dataclass("NullableSchema", schema) == expected.strip()


class RepeatedNestedField:
    int_field: List[int]


class RepeatedSchema:
    string_field: List[str]
    bytes_field: List[bytes]
    int_field: List[int]
    float_field: List[float]
    numeric_field: List[Decimal]
    bool_field: List[bool]
    timestamp_field: List[Timestamp]
    date_field: List[date]
    time_field: List[time]
    datetime_field: List[datetime]
    repeated_nested_field: List[RepeatedNestedField]


def test_repeated_schema_to_dataclass():
    schema = [
        SchemaField("string_field", "STRING", "REPEATED", None, ()),
        SchemaField("bytes_field", "BYTES", "REPEATED", None, ()),
        SchemaField("int_field", "INT64", "REPEATED", None, ()),
        SchemaField("float_field", "FLOAT64", "REPEATED", None, ()),
        SchemaField("numeric_field", "NUMERIC", "REPEATED", None, ()),
        SchemaField("bool_field", "BOOL", "REPEATED", None, ()),
        SchemaField("timestamp_field", "TIMESTAMP", "REPEATED", None, ()),
        SchemaField("date_field", "DATE", "REPEATED", None, ()),
        SchemaField("time_field", "TIME", "REPEATED", None, ()),
        SchemaField("datetime_field", "DATETIME", "REPEATED", None, ()),
        SchemaField(
            "repeated_nested_field",
            "STRUCT",
            "REPEATED",
            None,
            (SchemaField("int_field", "INT64", "REPEATED", None, ()),),
        ),
    ]
    expected = f"@dataclass\n{inspect.getsource(RepeatedNestedField)}\n@dataclass\n{inspect.getsource(RepeatedSchema)}"
    assert schema_to_dataclass("RepeatedSchema", schema) == expected.strip()
