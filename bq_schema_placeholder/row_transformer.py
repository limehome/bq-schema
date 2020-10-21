from dataclasses import asdict, dataclass
from typing import Generic, Type, TypeVar, Set, Dict, FrozenSet, get_type_hints, List

from google.cloud.bigquery.table import Row

from bq_schema_placeholder.dataclass_converter import dataclass_to_schema
from bq_schema_placeholder.types.type_parser import (
    parse_inner_type_of_list,
)


T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass(frozen=True)
class NestedField:
    column_name: str
    column_type: Type
    is_repeated: bool
    nested_fields: FrozenSet["NestedField"]


class RowTransformer(Generic[T]):
    def __init__(self, schema: Type[T]):
        self._schema: Type[T] = schema
        self._nested_fields: FrozenSet[NestedField] = self._get_nested_fields(
            self._schema
        )

    def bq_row_to_dataclass_instance(self, bq_row: Row) -> T:
        # the Row object does not support item assignment so we have to copy it
        row_as_dict = dict(bq_row)
        self._instantiate_nested_fields(row_as_dict, self._nested_fields)
        return self._schema(**row_as_dict)

    @staticmethod
    def dataclass_instance_to_bq_row(instance: T) -> dict:
        return asdict(instance)

    def _get_nested_fields(self, schema: Type[T]) -> FrozenSet[NestedField]:
        types = get_type_hints(schema)
        bq_schema = dataclass_to_schema(schema)
        nested_fields = set()
        for column in bq_schema:
            column_type = (
                types[column.name]
                if column.mode != "REPEATED"
                else parse_inner_type_of_list(types[column.name])
            )
            is_nested = column.field_type == "STRUCT"
            if is_nested:
                nested_fields.add(
                    NestedField(
                        column_name=column.name,
                        column_type=column_type,
                        is_repeated=column.mode == "REPEATED",
                        nested_fields=self._get_nested_fields(column_type)
                        if is_nested
                        else frozenset(),
                    )
                )
        return frozenset(nested_fields)

    def _instantiate_nested_fields(
        self, bq_row: dict, nested_fields: FrozenSet[NestedField]
    ) -> None:
        for nested_field in nested_fields:
            if nested_field.column_name in bq_row:
                if nested_field.is_repeated:
                    if nested_field.nested_fields:
                        for field in bq_row[nested_field.column_name]:
                            self._instantiate_nested_fields(
                                field, nested_field.nested_fields
                            )
                    bq_row[nested_field.column_name] = [
                        nested_field.column_type(**item)
                        for item in bq_row[nested_field.column_name]
                    ]
                else:
                    if nested_field.nested_fields:
                        self._instantiate_nested_fields(
                            bq_row[nested_field.column_name], nested_field.nested_fields
                        )
                    bq_row[nested_field.column_name] = nested_field.column_type(
                        **bq_row[nested_field.column_name]
                    )
