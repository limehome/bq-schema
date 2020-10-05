"""
Convert a bigquery schema into the source code for a data class.
"""
import itertools
import inspect
from typing import List, Type

from google.cloud.bigquery import SchemaField

from .types import BigQueryFieldModes, BigQueryTypes, TypeMapping

_TEMPLATE_DATACLASS = """
@dataclass
class {schema_name}:
{fields}
"""
_TEMPLATE_FIELD = "    {field_name}: {field_type}"


def schema_to_dataclass(schema_name: str, schema: List[SchemaField]) -> str:
    struct_fields = [s for s in schema if s.field_type == "STRUCT"]
    nested_dataclasses = "".join(
        schema_to_dataclass(s.name, s.fields) for s in struct_fields
    )

    dataclass_fields = [_create_field(field) for field in schema]
    python_dataclass = _TEMPLATE_DATACLASS.format(
        schema_name=schema_name, fields="\n".join(dataclass_fields)
    ).strip()
    return nested_dataclasses + python_dataclass


def _create_field(field: SchemaField) -> str:
    bigquery_type = BigQueryTypes[field.field_type]
    if bigquery_type == BigQueryTypes.STRUCT:
        dataclass_type = type(None).__name__
    else:
        dataclass_type = TypeMapping[bigquery_type].__name__

    field_mode = BigQueryFieldModes[field.mode]

    if field_mode == BigQueryFieldModes.REPEATED:
        dataclass_type = f"List[{dataclass_type}]"
    if field_mode == BigQueryFieldModes.NULLABLE:
        dataclass_type = f"Optional[{dataclass_type}]"

    return _TEMPLATE_FIELD.format(field_name=field.name, field_type=dataclass_type)
