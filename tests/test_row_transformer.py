from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import List, Optional

from google.cloud.bigquery.table import Row

from bq_schema.row_transformer import RowTransformer
from bq_schema.types.type_mapping import Timestamp


@dataclass
class NestedAgain:
    int_field: int


@dataclass
class NestedSchema:
    int_field: int
    nested_again: NestedAgain
    nested_again_optional_set: Optional[NestedAgain]
    nested_again_optional_unset: Optional[NestedAgain]
    nested_again_repeated: List[NestedAgain]


# pylint: disable=too-many-instance-attributes
@dataclass
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
    optional_int_unset: Optional[int]
    optional_int_set: Optional[int]
    optional_nested_field: Optional[NestedSchema]
    nested_field: NestedSchema
    nested_repeated_field: List[NestedSchema]
    datetime_field: datetime


def test_transform_dataclass_instance_to_row():

    instance = Schema(
        string_field="string",
        bytes_field=b"string",
        int_field=1,
        float_field=1.0,
        numeric_field=Decimal("1.0"),
        bool_field=True,
        timestamp_field=Timestamp(datetime(2020, 1, 1)),
        date_field=date(2020, 1, 1),
        time_field=time(1, 1, 1),
        optional_int_unset=None,
        optional_int_set=10,
        optional_nested_field=None,
        datetime_field=datetime(2020, 2, 2),
        nested_field=NestedSchema(
            int_field=2,
            nested_again=NestedAgain(int_field=3),
            nested_again_optional_set=NestedAgain(int_field=3),
            nested_again_optional_unset=None,
            nested_again_repeated=[NestedAgain(int_field=4)],
        ),
        nested_repeated_field=[
            NestedSchema(
                int_field=2,
                nested_again=NestedAgain(int_field=3),
                nested_again_optional_set=NestedAgain(int_field=3),
                nested_again_optional_unset=None,
                nested_again_repeated=[NestedAgain(int_field=4)],
            )
        ],
    )
    row_transformer = RowTransformer(Schema)
    expected = {
        "bool_field": True,
        "bytes_field": b"string",
        "date_field": date(2020, 1, 1),
        "datetime_field": datetime(2020, 2, 2, 0, 0),
        "float_field": 1.0,
        "int_field": 1,
        "nested_field": {
            "int_field": 2,
            "nested_again": {"int_field": 3},
            "nested_again_optional_set": {"int_field": 3},
            "nested_again_optional_unset": None,
            "nested_again_repeated": [{"int_field": 4}],
        },
        "nested_repeated_field": [
            {
                "int_field": 2,
                "nested_again": {"int_field": 3},
                "nested_again_optional_set": {"int_field": 3},
                "nested_again_optional_unset": None,
                "nested_again_repeated": [{"int_field": 4}],
            }
        ],
        "numeric_field": Decimal("1.0"),
        "string_field": "string",
        "time_field": time(1, 1, 1),
        "optional_int_set": 10,
        "optional_int_unset": None,
        "optional_nested_field": None,
        "timestamp_field": Timestamp(datetime(2020, 1, 1, 0, 0)),
    }
    assert row_transformer.dataclass_instance_to_bq_row(instance) == expected


def test_transfrom_row_into_dataclass():
    row_as_dict = {
        "bool_field": True,
        "bytes_field": b"string",
        "date_field": date(2020, 1, 1),
        "datetime_field": datetime(2020, 2, 2, 0, 0),
        "float_field": 1.0,
        "int_field": 1,
        "nested_field": {
            "int_field": 2,
            "nested_again": {"int_field": 3},
            "nested_again_optional_set": {"int_field": 3},
            "nested_again_repeated": [{"int_field": 4}],
        },
        "nested_repeated_field": [
            {
                "int_field": 2,
                "nested_again": {"int_field": 3},
                "nested_again_optional_set": {"int_field": 3},
                "nested_again_repeated": [{"int_field": 4}],
            }
        ],
        "numeric_field": Decimal("1.0"),
        "string_field": "string",
        "time_field": time(1, 1, 1),
        "optional_int_set": 10,
        "timestamp_field": Timestamp(datetime(2020, 1, 1, 0, 0)),
    }
    row_transformer = RowTransformer(Schema)
    row = dict_to_row(row_as_dict)

    expected = Schema(
        string_field="string",
        bytes_field=b"string",
        int_field=1,
        float_field=1.0,
        numeric_field=Decimal("1.0"),
        bool_field=True,
        timestamp_field=Timestamp(datetime(2020, 1, 1)),
        date_field=date(2020, 1, 1),
        time_field=time(1, 1, 1),
        optional_int_unset=None,
        optional_int_set=10,
        optional_nested_field=None,
        datetime_field=datetime(2020, 2, 2),
        nested_field=NestedSchema(
            int_field=2,
            nested_again=NestedAgain(int_field=3),
            nested_again_optional_set=NestedAgain(int_field=3),
            nested_again_optional_unset=None,
            nested_again_repeated=[NestedAgain(int_field=4)],
        ),
        nested_repeated_field=[
            NestedSchema(
                int_field=2,
                nested_again=NestedAgain(int_field=3),
                nested_again_optional_set=NestedAgain(int_field=3),
                nested_again_optional_unset=None,
                nested_again_repeated=[NestedAgain(int_field=4)],
            )
        ],
    )
    assert expected == row_transformer.bq_row_to_dataclass_instance(row)


def dict_to_row(row_as_dict: dict) -> Row:
    values = []
    field_to_index = {}
    for i, (key, value) in enumerate(row_as_dict.items()):
        values.append(value)
        field_to_index[key] = i

    return Row(values=values, field_to_index=field_to_index)
