from dataclasses import is_dataclass
from typing import Union, List, Type, Optional, cast
from google.cloud.bigquery import SchemaField, TimePartitioning
from bq_schema_placeholder.dataclass_converter import dataclass_to_schema


class BigqueryTable:
    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def schema(self) -> Union[List[SchemaField], Type]:
        raise NotImplementedError

    @property
    def project(self) -> Optional[str]:
        return None

    @property
    def dataset(self) -> Optional[str]:
        return None

    @property
    def version(self) -> Optional[str]:
        return None

    @property
    def time_partitioning(self) -> Optional[TimePartitioning]:
        return None

    def full_table_name(self) -> str:
        if self.version:
            return f"{self.name}_v{self.version}"

        return self.name

    def get_schema_fields(self) -> List[SchemaField]:
        if is_dataclass(self.schema):
            return dataclass_to_schema(cast(Type, self.schema))

        return cast(List[SchemaField], self.schema)
