from typing import Iterable, Type, TypeVar, Union

T = TypeVar('T')


def filter_type(
    iterable: Union[None, T, Iterable[T]],
    /,
    type: Type[T] = object,
) -> Iterable[T]:
    return (
        iterable
        if isinstance(iterable, Iterable) and not isinstance(iterable, type)
        else (iterable,)
        if isinstance(iterable, type)
        else ()
    )
