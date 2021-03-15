from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, NewType, Type
from geodaisy import GeoObject

from .big_query_types import BigQueryTypes

Timestamp = NewType("Timestamp", datetime)


class Geography(GeoObject):
    def __init__(self, geo_string: str) -> None:
        index_of_open_parens = geo_string.find("(")
        if geo_string[index_of_open_parens - 1] != " ":
            geo_string = (
                geo_string[:index_of_open_parens]
                + " "
                + geo_string[index_of_open_parens:]
            )
        super().__init__(geo_string)

    def __str__(self) -> str:
        return self.wkt()

    def __repr__(self) -> str:
        return self.wkt()


TypeMapping: Dict[BigQueryTypes, Type] = {
    BigQueryTypes.STRING: str,
    BigQueryTypes.BYTES: bytes,
    BigQueryTypes.INT64: int,
    BigQueryTypes.FLOAT64: float,
    BigQueryTypes.NUMERIC: Decimal,
    BigQueryTypes.BOOL: bool,
    BigQueryTypes.TIMESTAMP: Timestamp,
    BigQueryTypes.DATE: date,
    BigQueryTypes.TIME: time,
    BigQueryTypes.DATETIME: datetime,
    BigQueryTypes.GEOGRAPHY: Geography,
}
