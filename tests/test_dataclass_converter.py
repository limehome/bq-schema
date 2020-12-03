# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import List, Optional

from google.cloud.bigquery import SchemaField

from bq_schema.dataclass_converter import dataclass_to_schema
from bq_schema.types.type_mapping import Timestamp


def test_types():
    @dataclass
    class NestedSchema:
        int_field: int = field(metadata={"description": "This is an INT field."})

    @dataclass
    class Schema:
        string_field: str = field(metadata={"description": "This is a STRING field."})
        bytes_field: bytes
        int_field: int
        float_field: float
        numeric_field: Decimal
        bool_field: bool
        timestamp_field: Timestamp
        date_field: date
        time_field: time
        datetime_field: datetime
        nested_field: NestedSchema = field(
            metadata={"description": "This is a STRUCT field."}
        )

    assert dataclass_to_schema(Schema) == [
        SchemaField(
            "string_field", "STRING", "REQUIRED", "This is a STRING field.", ()
        ),
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
            "nested_field",
            "STRUCT",
            "REQUIRED",
            "This is a STRUCT field.",
            (
                SchemaField(
                    "int_field", "INT64", "REQUIRED", "This is an INT field.", ()
                ),
            ),
        ),
    ]


def test_optional_types():
    @dataclass
    class NestedSchema:
        int_field: Optional[int] = field(
            metadata={"description": "This is an INT field."}
        )

    @dataclass
    class Schema:
        string_field: Optional[str] = field(
            metadata={"description": "This is a STRING field."}
        )
        bytes_field: Optional[bytes]
        int_field: Optional[int]
        float_field: Optional[float]
        numeric_field: Optional[Decimal]
        bool_field: Optional[bool]
        timestamp_field: Optional[Timestamp]
        date_field: Optional[date]
        time_field: Optional[time]
        datetime_field: Optional[datetime]
        nested_field: Optional[NestedSchema] = field(
            metadata={"description": "This is a STRUCT field."}
        )

    assert dataclass_to_schema(Schema) == [
        SchemaField(
            "string_field", "STRING", "NULLABLE", "This is a STRING field.", ()
        ),
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
            "nested_field",
            "STRUCT",
            "NULLABLE",
            "This is a STRUCT field.",
            (
                SchemaField(
                    "int_field", "INT64", "NULLABLE", "This is an INT field.", ()
                ),
            ),
        ),
    ]


def test_repeated_types():
    @dataclass
    class NestedSchema:
        int_field: List[int] = field(metadata={"description": "This is an INT field."})

    @dataclass
    class Schema:
        string_field: List[str] = field(
            metadata={"description": "This is a STRING field."}
        )
        bytes_field: List[bytes]
        int_field: List[int]
        float_field: List[float]
        numeric_field: List[Decimal]
        bool_field: List[bool]
        timestamp_field: List[Timestamp]
        date_field: List[date]
        time_field: List[time]
        datetime_field: List[datetime]
        nested_field: List[NestedSchema] = field(
            metadata={"description": "This is a STRUCT field."}
        )

    assert dataclass_to_schema(Schema) == [
        SchemaField(
            "string_field", "STRING", "REPEATED", "This is a STRING field.", ()
        ),
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
            "nested_field",
            "STRUCT",
            "REPEATED",
            "This is a STRUCT field.",
            (
                SchemaField(
                    "int_field", "INT64", "REPEATED", "This is an INT field.", ()
                ),
            ),
        ),
    ]
