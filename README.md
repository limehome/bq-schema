# bq-schema
![Python package](https://github.com/limehome/bq-schema/workflows/Python%20package/badge.svg)
[![PyPI version](https://badge.fury.io/py/bq_schema.svg)](https://badge.fury.io/py/bq_schema)
![Codecov](https://img.shields.io/codecov/c/github/limehome/bq-schema)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)


## Motivation

At limehome we are heavy users of python and bigquery. This library was created to mainly solve the following issues:

* Define table schemas in code and have a migration script to apply changes.
* On deploy make sure that all schemas were applied, otherwise abort.
* Guarantee that when we try to write data to a table, the data matches the schema of the table (required / optional, datatypes)
* Version our tables and enable migrations to a new schema

Additionally this library aims to help the users through the usage of [python typing](https://docs.python.org/3/library/typing.html).

* Specify your schema as a [python dataclass](https://docs.python.org/3/library/dataclasses.html)
* Our migration script converts the data class into a bigquery schema definition
* Deserialize rows into a dataclass instance, while reading from a table
* Serialize a dataclass instance into a dictionary and write it to the table.

The main benefit of combining all these features is, that we can guarantee that our code will run, before we deploy to production.



## Quickstart
Since this library makes use of newer features of python, you need at least python3.7.

1. Install the package
```
pip install bq_schema
```

2. Create a schema and a table definition in my_table.py
```python
@dataclass
class Schema:
    string_field: str = field(metadata={"description": "This is a STRING field."})
    int_field: Optional[int]
    some_floats: List[float]
    bool_field: bool

class MyTable(BigqueryTable):
    name = "my_table_name"
    schema = Schema
```

If you have already tables created in your account, you can use the convert-table script to create a schema.

3. Create your table

**Hint:** 
Make sure to have you credentials set:
```
export GOOGLE_APPLICATION_CREDENTIALS=your_auth.json
```

Alternativly you can set the service_file as a environment variable:
```
export GOOGLE_SERVICE_FILE={"type": "service_account", ...}
```

Now create your table
```
migrate-tables --project my_project --dataset my_dataset --module-path my_table --apply
```

4. Write a row
```python
from google.cloud import bigquery
row = Schema(string_field="foo", int_field=1, some_floats=[1.0, 2.0], bool_field=True)
row_transformer = RowTransformer[Schema](Schema)
serialized_row = RowTransformer.dataclass_instance_to_bq_row(row)

bigquery_client = bigquery.Client()
table = bigquery_client.get_table("project.dataset.my_table_name")
bigquery_client.insert_rows(table, [serialized_row])
```

5. Validate you code with a type checker like [mypy](http://mypy-lang.org/)
```
mypy my_table.py
```

6. Read a row
```python
query = "SELECT * FROM project.dataset.my_table_name"
for row in bigquery_client.query(query=query):
    deserialized_row = row_transformer.bq_row_to_dataclass_instance(row)
    assert isinstance(deserialized_row, Schema)
```


## Documentation

### Schema definitions
For a full list of supported types check the following schema:
```python
from typing import Optional
from dataclasses import dataclass
from bq_schema.types.type_mapping import Timestamp, Geography

@dataclass
class RequiredNestedField:
    int_field: int = field(metadata={"description": "This field is an INT field."})


@dataclass
class RequiredSchema:
    string_field: str = field(metadata={"description": "This field is a STRING field."})
    string_field_optional = Optional[str]
    bytes_field: bytes
    int_field: int
    float_field: float
    numeric_field: Decimal
    bool_field: bool
    timestamp_field: Timestamp
    date_field: date
    time_field: time
    datetime_field: datetime
    geography_field: Geography
    required_nested_field: RequiredNestedField = field(metadata={"description": "This field is a STRUCT field."})
    optional_nested_field: Optional[RequiredNestedField] 
    repeated_nested_field: List[RequiredNestedField]
```

#### Timestamps
Timestamps are deserialized into datetime objects, due to the nature of the underlying bq library. To distinguish between datetime and timestamp use bq_schema.types.type_mapping.
Usage:

```python
from bq_schema.types.type_mapping import Timestamp
from datetime import datetime

the_timestamp = Timestamp(datetime.utcnow())
```


#### Geography
This library treats the geography data type as a string. BigQuery accepts geography values either in the [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) or [GeoJson](https://geojson.org) format. To actually parse and work with geodata in python, one could use the [shapely](https://pypi.org/project/Shapely/) library. Here is an example how to load a point from the WKT format:
```python
from shapely.wkt import loads
loads('POINT (0 0)')
```

### Table definitions
The bigquery class is used for:
* Recursive table discovery by our migrate-tables script
* Define table properties like name and schema


### Required properties
* name: The name of the table
* schema: table schema either as dataclass or a list of schema fields

### Optional properties
* project: name of the project, can be overwritten by the migrate-tables script
* dataset: name of the dataset, can be overwritten by the migrate-tables script

#### Versioning tables
Since bigquery does not allow backwards incompatible schema changes, you might want to version your schemas.project
```python
class MyTable(BigqueryTable):
    name = "my_table_name"
    schema = Schema
    version = "1"
```
By default the version will be appended to the table name, like so: my_table_name_v1. If you want to overwrite this behaviour,
you can implement the full_table_name method.

#### Time partitioning
Define time partitioning for your table:
```python
from bq_schema.types.type_mapping import Timestamp
from google.cloud.bigquery import TimePartitioning, TimePartitioningType

class MyTable:
    time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY, field="some_column"
    )
```

### Scripts

#### migrate-tables
This script has two uses:
* Check if locally defined schemas are in sync with the schemas in bigquery
* If a difference is detected, we try to apply the changes

The script will find all defined tables recursivly for a given python module.

**Note**: If you have not defined your project and / or dataset in code, you will have to pass it as a parameter to the script.
Show the help:
```
migrate-tables --help
```

Check if tables are in sync. List all changes.
```
migrate-tables --module-path module/
```

If you want the script to fail on a change, add the validate flag. Useful for running inside your CI:
```
migrate-tables --module-path module/ --validate
```

Apply changes
```
migrate-tables --module-path src/jobs/ --apply
```


#### convert-table
If you already have tables created in bigquery, this script print the corresponding dataclass for you.

Show the help:
```
convert-table --help
```

Print a table:
```
convert-table --project project --dataset scraper --table-name table_name >> schema.py
```

### Development

#### Setting up your dev environment
1) Clone the project.
2) Navigate into the cloned project.
3) Create a virtual environment with **python version >=3.7**

    `pipenv --python PYTHON_VERSION`
    ```
    $ pipenv --python 3.7
    ```
    or

    `virtualenv -p /PATH_TO_PYTHON/ /DESIRED_PATH/VENV_NAME`
    ```
    $ virtualenv -p /usr/bin/python3.7 placeholder
    ```

4) Install flit via pip
    ```
    $ pip install flit
    ```
5) Install packages

    ```
    $ flit install --symlink
    ```

#### Code quality
Run all code quality checks:
```
inv check-all
```
##### Test
```
inv test
```
##### Lint
```
inv lint
```
##### Types
```
inv type-check
```
##### Code format
```
inv format-code
```

Validate code is correctly formatted:
```
inv check-code-format
```
