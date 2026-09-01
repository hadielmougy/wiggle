"""Conversions between plain Python values and protobuf Struct/Value (used for workflow
definitions and instance contexts, which travel as JSON-native google.protobuf.Struct/Value)."""
from __future__ import annotations

from typing import Any

from google.protobuf import json_format, struct_pb2
from google.protobuf.message import Message


def to_struct(d: dict) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    json_format.ParseDict(d, s)
    return s


def to_value(v: Any) -> struct_pb2.Value:
    out = struct_pb2.Value()
    json_format.ParseDict(v, out)
    return out


def from_value(v: struct_pb2.Value) -> Any:
    return json_format.MessageToDict(v)


def msg_to_dict(m: Message) -> dict:
    return json_format.MessageToDict(m, preserving_proto_field_name=True)
