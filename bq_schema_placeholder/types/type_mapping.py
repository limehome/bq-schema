from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, Type

from .big_query_types import BigQueryTypes


class Timestamp(datetime):
    pass


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
}
