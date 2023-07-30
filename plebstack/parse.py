from typing import cast

from glom import glom, Iter, T

from plebstack.models import Response


def expect_nonempty(messages: list):
    """Raise ValueError if messages is empty"""
    if len(messages) == 0:
        msg = f"empty message: {messages=}"
        raise ValueError(msg)


def records_jl(
    messages: list[dict], response_t: type[Response], columns: dict[str, str]
) -> list[dict]:
    """Validate a list of parsed JSON lines into a list of records

    Parameters
    ----------
    messages : list[dict]
        List of JSON lines
    response_t : type[Response]
        Response type to parse
    columns : dict[str, str]
        Columns to include in a record: {"column_name": "<API attr>"}

    Returns
    -------
    list[dict]
        List of records
    """
    expect_nonempty(messages)
    res = glom(
        messages,
        [Iter(response_t.model_validate).map(T.as_records(columns)).flatten().all()],
    )
    return cast(list[dict], res)


def records(
    messages: list[str], response_t: type[Response], columns: dict[str, str]
) -> list[dict]:
    """Parse a list of JSON responses into a list of records

    Parameters
    ----------
    messages : list[str]
        List of JSON responses
    response_t : type[Response]
        Response type to parse
    columns : dict[str, str]
        Columns to include in a record: {"column_name": "<API attr>"}

    Returns
    -------
    list[dict]
        List of records
    """
    expect_nonempty(messages)
    res = glom(
        messages,
        Iter(response_t.select).filter(T).map(T.as_records(columns)).flatten().all(),
    )
    return cast(list[dict], res)


def objects(messages: list[str], response_t: type[Response]) -> list[Response]:
    """Parse a list of JSON responses into a list of objects"""
    expect_nonempty(messages)
    res = glom(messages, Iter(response_t.select).filter(T).all())
    return cast(list[Response], res)
