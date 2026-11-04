"""JSON serialization for inputs, step outputs and signals.

JSON (not pickle) keeps the journal readable in the dashboard and safe to load.
Dataclasses, datetimes, dates, Decimals, sets, tuples, enums and objects with a
``model_dump()`` method (pydantic) are converted; everything round-trips as plain JSON types.
"""

from __future__ import annotations

import dataclasses
import json
import traceback
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from .errors import SerializationError


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = sorted(value, key=repr) if isinstance(value, (set, frozenset)) else value
        return [to_jsonable(v) for v in items]
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(dataclasses.asdict(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return to_jsonable(value.value)
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return to_jsonable(dump(mode="json"))
    raise SerializationError(
        f"cannot serialize {type(value).__name__}; return JSON-compatible values, dataclasses or pydantic models"
    )


def dumps(value: Any) -> str:
    return json.dumps(to_jsonable(value), separators=(",", ":"), ensure_ascii=False)


def loads(text: str | None) -> Any:
    if text is None:
        return None
    return json.loads(text)


def error_dict(exc: BaseException) -> dict[str, Any]:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return {"type": type(exc).__name__, "message": str(exc), "traceback": tb[-4000:]}
