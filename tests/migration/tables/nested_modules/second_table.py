from google.cloud.bigquery import SchemaField
from bq_schema_placeholder.bigquery_table import BigqueryTable


class SecondTable(BigqueryTable):
    name = "second_table"
    schema = [SchemaField("b_column", "FLOAT")]
