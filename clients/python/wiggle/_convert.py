"""Conversions between Python values and the protobuf ``Value`` used on the wire, plus the
shallow-diff merge the engine expects from a completed step."""
from __future__ import annotations

from typing import Any

from google.protobuf import struct_pb2


def to_value(obj: Any) -> struct_pb2.Value:
    """Convert a JSON-shaped Python object into a protobuf ``Value``."""
    v = struct_pb2.Value()
    if obj is None:
        v.null_value = struct_pb2.NULL_VALUE
    elif isinstance(obj, bool):
        v.bool_value = obj
    elif isinstance(obj, (int, float)):
        v.number_value = float(obj)
    elif isinstance(obj, str):
        v.string_value = obj
    elif isinstance(obj, dict):
        v.struct_value.SetInParent()
        for k, item in obj.items():
            v.struct_value.fields[str(k)].CopyFrom(to_value(item))
    elif isinstance(obj, (list, tuple)):
        v.list_value.SetInParent()
        for item in obj:
            v.list_value.values.append(to_value(item))
    else:
        raise TypeError(f"cannot serialise {type(obj).__name__} to a wiggle context value")
    return v


def from_value(v: struct_pb2.Value) -> Any:
    """Convert a protobuf ``Value`` back into a plain Python object.

    Whole numbers come back as ``int`` (proto only has doubles) so contexts read naturally.
    """
    kind = v.WhichOneof("kind")
    if kind is None or kind == "null_value":
        return None
    if kind == "bool_value":
        return v.bool_value
    if kind == "number_value":
        n = v.number_value
        return int(n) if n.is_integer() else n
    if kind == "string_value":
        return v.string_value
    if kind == "struct_value":
        return {k: from_value(val) for k, val in v.struct_value.fields.items()}
    if kind == "list_value":
        return [from_value(item) for item in v.list_value.values]
    raise ValueError(f"unexpected value kind: {kind}")


def shallow_diff(before: Any, after: Any) -> Any:
    """The top-level delta to merge back into the context: changed/added keys, and ``None`` for
    keys the step dropped. Mirrors the server's merge semantics, so a step returns the *whole*
    context and only what actually changed is written."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return after
    delta: dict[str, Any] = {}
    for k, val in after.items():
        if before.get(k) != val:
            delta[k] = val
    for k in before:
        if k not in after:
            delta[k] = None
    return delta
