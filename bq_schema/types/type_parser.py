from typing import Any, Type

from typing_extensions import get_args

_NoneType = type(None)


def parse_inner_type_of_list(list_type: Any) -> Type:
    return get_args(list_type)[0]


def parse_inner_type_of_optional(optional_type: Any) -> Type:
    args = tuple(arg for arg in get_args(optional_type) if arg not in [_NoneType, _NULL_TYPE])

    if len(args) != 1:
        raise TypeError(f"Unsupported type: {optional_type}.")

    return next(arg for arg in args)
