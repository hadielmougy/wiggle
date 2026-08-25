"""Wiggle — Python client and worker for the Wiggle workflow engine.

    from wiggle import Workflow, Retry, WiggleClient, Worker

Define a workflow, register it, start instances from the client, and process steps with a worker.
Talks the same gRPC control plane as the Java client, so Python and Java workers interoperate.
"""
from .client import InstanceView, ScheduleView, WiggleClient
from .worker import PermanentError, Worker
from .workflow import Blueprint, Branch, Case, Retry, Workflow

__all__ = [
    "Workflow",
    "Branch",
    "Case",
    "Retry",
    "Blueprint",
    "WiggleClient",
    "InstanceView",
    "ScheduleView",
    "Worker",
    "PermanentError",
]

__version__ = "0.1.0"
