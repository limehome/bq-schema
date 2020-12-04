from typing import Any, Type

_NoneType = type(None)


def parse_inner_type_of_list(list_type: Any) -> Type:
    return list_type.__args__[0]


def parse_inner_type_of_optional(optional_type: Any) -> Type:

    args = optional_type.__args__
    if not (len(args) == 2 and any(arg is _NoneType for arg in args)):
        raise TypeError(f"Unsupported type: {optional_type}.")

    return next(arg for arg in args if arg is not _NoneType)
