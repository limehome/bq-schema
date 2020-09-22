"""
Convert a python dataclass into a BigQuery schema definition.
"""
from dataclasses import Field, fields, is_dataclass
from typing import Any, List, Type, Union

from google.cloud.bigquery import SchemaField

from .types import BigQueryFieldModes, BigQueryTypes, TypeMapping

_BASIC_TYPES_TO_NAME = {
    primitive_type: bq_type for bq_type, primitive_type in TypeMapping.items()
}
_NoneType = type(None)


def dataclass_to_schema(dataclass: Type) -> List[SchemaField]:
    if not is_dataclass(dataclass):
        raise TypeError("Not a dataclass.")

    return [_field_to_schema(field) for field in fields(dataclass)]


def _field_to_schema(field: Field) -> SchemaField:
    field_type = _BASIC_TYPES_TO_NAME.get(field.type)
    if field_type:
        return SchemaField(
            name=field.name,
            field_type=field_type,
            mode=BigQueryFieldModes.REQUIRED,
        )

    if is_dataclass(field.type):
        return SchemaField(
            name=field.name,
            field_type=BigQueryTypes.STRUCT,
            mode=BigQueryFieldModes.REQUIRED,
            fields=_parse_fields(field.type),
        )

    # typing.Optional is the same as typing.Union[SomeType, NoneType]
    if field.type.__origin__ is Union:
        return _parse_optional(field)

    if field.type.__origin__ is list:
        return _parse_list(field)

    raise TypeError(f"Unsupported type: {field.type}.")


def _parse_list(field: Field) -> SchemaField:
    field_type = field.type.__args__[0]
    return SchemaField(
        name=field.name,
        field_type=_python_type_to_big_query_type(field_type),
        mode=BigQueryFieldModes.REPEATED,
        fields=_parse_fields(field_type),
    )


def _parse_optional(field: Field) -> SchemaField:
    args = field.type.__args__
    if not (len(args) == 2 and any(arg is _NoneType for arg in args)):
        raise TypeError(f"Unsupported type: {field.type}.")

    field_type = next(arg for arg in args if arg is not _NoneType)
    return SchemaField(
        name=field.name,
        field_type=_python_type_to_big_query_type(field_type),
        mode=BigQueryFieldModes.NULLABLE,
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
