"""Exact App ID parsing for the Bronze snapshot and its preflight."""

from decimal import Decimal, InvalidOperation
import re


_NUMERIC_APPID = re.compile(r"\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def canonical_appid(raw):
    """Accept exact positive int64 keys, including legacy numeric notation."""
    value = raw.strip()
    if not _NUMERIC_APPID.fullmatch(value):
        raise ValueError("Invalid Steam App ID")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError("Invalid Steam App ID") from None
    if not 0 < number < 2**63 or number != number.to_integral_value():
        raise ValueError("Invalid Steam App ID")
    return str(int(number))
