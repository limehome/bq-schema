"""
Convert a python dataclass into a BigQuery schema definition.
"""
from dataclasses import Field, fields, is_dataclass
from typing import Any, List, Optional, Type, Union, get_type_hints

from google.cloud.bigquery import SchemaField
from typing_extensions import get_origin

from bq_schema.types.type_parser import (
    parse_inner_type_of_list,
    parse_inner_type_of_optional,
)

from .types import BigQueryFieldModes, BigQueryTypes, TypeMapping

_BASIC_TYPES_TO_NAME = {
    primitive_type: bq_type for bq_type, primitive_type in TypeMapping.items()
}
_NoneType = type(None)


def dataclass_to_schema(
    dataclass: Type, localns: Optional[dict] = None
) -> List[SchemaField]:
    """
    Transfrom a dataclass into a list of SchemaField.

    If you want to transform a dataclass that is not defined in the
    global scope you need to pass your locals.

    def my_func():
        @dataclass
        class Example1:
            a: int

        @dataclass
        class Example2:
            b: Example1

        dataclass_to_schema(Example2, localns=locals())
    """
    if not is_dataclass(dataclass):
        raise TypeError("Not a dataclass.")

    type_hints = get_type_hints(dataclass, localns=localns)
    dataclass_fields = fields(dataclass)
    for field in dataclass_fields:
        field.type = type_hints[field.name]
    return [_field_to_schema(field) for field in dataclass_fields]


def _field_to_schema(field: Field) -> SchemaField:
    field_type = _BASIC_TYPES_TO_NAME.get(field.type)
    if field_type:
        return SchemaField(
            name=field.name,
            field_type=field_type,
            description=_parse_field_description(field),
            mode=BigQueryFieldModes.REQUIRED,
        )

    if is_dataclass(field.type):
        return SchemaField(
            name=field.name,
            field_type=BigQueryTypes.STRUCT,
            mode=BigQueryFieldModes.REQUIRED,
            description=_parse_field_description(field),
            fields=_parse_fields(field.type),
        )

    # typing.Optional is the same as typing.Union[SomeType, NoneType]
    if get_origin(field.type) is Union:
        return _parse_optional(field)

    if get_origin(field.type) is list:
        return _parse_list(field)

    raise TypeError(f"Unsupported type: {field.type}.")


def _parse_list(field: Field) -> SchemaField:
    field_type = parse_inner_type_of_list(field.type)
    return SchemaField(
        name=field.name,
        field_type=_python_type_to_big_query_type(field_type),
        mode=BigQueryFieldModes.REPEATED,
        description=_parse_field_description(field),
        fields=_parse_fields(field_type),
    )


def _parse_optional(field: Field) -> SchemaField:
    field_type = parse_inner_type_of_optional(field.type)
    return SchemaField(
        name=field.name,
        field_type=_python_type_to_big_query_type(field_type),
        mode=BigQueryFieldModes.NULLABLE,
        description=_parse_field_description(field),
        fields=_parse_fields(field_type),
    )


def _parse_fields(field_type: Type) -> List[SchemaField]:
    if is_dataclass(field_type):
        return dataclass_to_schema(field_type)

    return []


def _python_type_to_big_query_type(field_type: Any) -> BigQueryTypes:
    if is_dataclass(field_type):
        return BigQueryTypes.STRUCT

    bq_type = _BASIC_TYPES_TO_NAME.get(field_type)
    if bq_type:
        return bq_type

    raise TypeError(f"Unsupported type: {field_type}")


def _parse_field_description(field: Field) -> Optional[str]:
    if "description" in field.metadata:
        return field.metadata["description"]
    return None
