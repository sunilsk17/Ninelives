"""Turn plain Python functions into LLM tools: JSON schema from type hints and docstrings."""

from __future__ import annotations

import enum
import inspect
import re
import types
import typing
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Union, get_args, get_origin

from ..types import RetryPolicy

_PRIMITIVES: dict[Any, dict[str, Any]] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    dict: {"type": "object"},
    list: {"type": "array"},
    Any: {},
}


def json_schema(tp: Any) -> dict[str, Any]:
    """JSON schema for a Python type annotation (the common cases tools need)."""
    if tp in _PRIMITIVES:
        return dict(_PRIMITIVES[tp])
    origin = get_origin(tp)
    args = get_args(tp)
    if origin is Literal:
        values = list(args)
        schema = json_schema(type(values[0])) if values else {}
        return {**schema, "enum": values}
    if origin in (Union, types.UnionType):
        non_null = [a for a in args if a is not type(None)]
        if len(non_null) == 1:
            return json_schema(non_null[0])
        return {"anyOf": [json_schema(a) for a in non_null]}
    if origin in (list, tuple, set, frozenset) or tp in (tuple, set):
        item = args[0] if args else Any
        return {"type": "array", "items": json_schema(item)}
    if origin is dict:
        return {"type": "object", "additionalProperties": json_schema(args[1]) if len(args) == 2 else {}}
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        return {"enum": [m.value for m in tp]}
    return {}


def _param_docs(doc: str) -> dict[str, str]:
    """Parse ``name: description`` lines from Google/Sphinx style docstrings."""
    out: dict[str, str] = {}
    for line in doc.splitlines():
        m = re.match(r"\s*(?::param\s+)?(\w+)\s*(?:\([^)]*\))?\s*:\s*(.+)", line)
        if m and m.group(1) not in {"Args", "Returns", "Raises", "Example", "Examples"}:
            out[m.group(1)] = m.group(2).strip()
    return out


@dataclass
class Tool:
    name: str
    description: str
    fn: Callable[..., Any]
    input_schema: dict[str, Any]
    #: Wait for a human approval signal before running.
    requires_approval: bool = False
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: float | None = None

    def spec(self) -> dict[str, Any]:
        """Tool definition in Messages API form."""
        return {"name": self.name, "description": self.description, "input_schema": self.input_schema}


def tool(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    requires_approval: bool = False,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
) -> Any:
    """Declare a tool. The docstring's first paragraph becomes the description and
    ``param: text`` lines describe parameters.

    >>> @tool(requires_approval=True)
    ... def refund(order_id: str, amount_cents: int) -> dict:
    ...     '''Refund an order.
    ...     order_id: the order to refund
    ...     amount_cents: amount in cents'''
    """

    def build(f: Callable[..., Any]) -> Tool:
        return make_tool(
            f,
            name=name,
            description=description,
            requires_approval=requires_approval,
            retry=retry,
            timeout=timeout,
        )

    return build(fn) if fn is not None else build


def make_tool(
    f: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
    requires_approval: bool = False,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
) -> Tool:
    doc = inspect.getdoc(f) or ""
    summary = doc.split("\n\n")[0].strip() if doc else f.__name__.replace("_", " ")
    pdocs = _param_docs(doc)
    try:
        hints = typing.get_type_hints(f)
    except Exception:  # an annotation that cannot be resolved (e.g. a local class) degrades to "any"
        hints = {}
    props: dict[str, Any] = {}
    required: list[str] = []
    for pname, param in inspect.signature(f).parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        schema = json_schema(hints.get(pname, Any))
        if pname in pdocs:
            schema["description"] = pdocs[pname]
        props[pname] = schema
        if param.default is inspect.Parameter.empty:
            required.append(pname)
    return Tool(
        name=name or f.__name__,
        description=description or summary,
        fn=f,
        input_schema={"type": "object", "properties": props, "required": required},
        requires_approval=requires_approval,
        retry=retry or RetryPolicy(),
        timeout=timeout,
    )
