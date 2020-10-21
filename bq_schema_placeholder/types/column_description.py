from dataclasses import Field, field


def description(column_description: str) -> Field:
    return field(metadata={"description": column_description})
