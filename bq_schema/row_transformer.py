from dataclasses import asdict
from typing import TypeVar, Generic, Type
from dacite import from_dict, Config

from google.cloud.bigquery.table import Row


T = TypeVar("T")  # pylint: disable=invalid-name


class RowTransformer(Generic[T]):
    def __init__(self, schema: Type[T]):
        self._schema: Type[T] = schema

    def bq_row_to_dataclass_instance(self, bq_row: Row) -> T:
        return from_dict(self._schema, bq_row, config=Config(check_types=False))

    @staticmethod
    def dataclass_instance_to_bq_row(instance: T) -> dict:
        return asdict(instance)
