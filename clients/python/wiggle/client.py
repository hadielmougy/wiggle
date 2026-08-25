"""The control-plane client: register workflows, start and track instances, deliver signals,
manage schedules. Also carries the low-level worker RPCs (poll/complete/fail/heartbeat) used by
:class:`~wiggle.worker.Worker`."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Union

import grpc
from google.protobuf import json_format, struct_pb2

from ._convert import from_value, to_value
from ._proto import wiggle_pb2 as pb
from ._proto import wiggle_pb2_grpc as rpc
from .workflow import Blueprint

TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED"})


@dataclass
class InstanceView:
    id: str
    workflow: str
    version: int
    status: str
    termination_reason: Optional[str]
    error: Optional[str]
    context: Any
    created_at: int
    updated_at: int

    @property
    def done(self) -> bool:
        return self.status in TERMINAL


@dataclass
class ScheduleView:
    id: str
    workflow: str
    every_millis: int
    cron: str
    next_fire_at: int
    created_at: int


class WiggleClient:
    """A thin, Pythonic wrapper over the gRPC control plane. Use as a context manager."""

    def __init__(self, target: str = "localhost:8080", *,
                 credentials: Optional[grpc.ChannelCredentials] = None):
        self._channel = (grpc.secure_channel(target, credentials) if credentials
                         else grpc.insecure_channel(target))
        self._stub = rpc.WiggleControlPlaneStub(self._channel)

    def __enter__(self) -> "WiggleClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._channel.close()

    # ---- workflows & instances ----

    def register(self, blueprint: Blueprint) -> int:
        """Register a workflow definition; returns its version. Idempotent for the same graph."""
        struct = json_format.ParseDict(blueprint.definition, struct_pb2.Struct())
        result = self._stub.RegisterWorkflow(pb.WorkflowDefinition(definition=struct))
        return int(result.version)

    def start(self, workflow: Union[Blueprint, str], context: Any, *,
              version: Optional[int] = None, correlation_id: Optional[str] = None) -> str:
        """Start an instance; returns its id."""
        name = workflow.name if isinstance(workflow, Blueprint) else workflow
        req = pb.StartInstanceRequest(workflow=name, context=to_value(context))
        if version is not None:
            req.version = version
        if correlation_id is not None:
            req.correlation_id = correlation_id
        return self._stub.StartInstance(req).instance_id

    def instance(self, instance_id: str) -> InstanceView:
        return _view(self._stub.GetInstance(pb.InstanceIdRequest(instance_id=instance_id)).instance)

    def await_completion(self, instance_id: str, timeout_s: float = 30.0,
                         poll_interval_s: float = 0.2) -> InstanceView:
        """Poll until the instance reaches a terminal state, or raise ``TimeoutError``."""
        deadline = time.monotonic() + timeout_s
        while True:
            view = self.instance(instance_id)
            if view.done:
                return view
            if time.monotonic() >= deadline:
                raise TimeoutError(f"instance {instance_id} still {view.status} after {timeout_s}s")
            time.sleep(poll_interval_s)

    def list_instances(self, *, workflow: Optional[str] = None, status: Optional[str] = None,
                       limit: int = 100) -> list[InstanceView]:
        req = pb.ListInstancesRequest(limit=limit)
        if workflow is not None:
            req.workflow = workflow
        if status is not None:
            req.status = status
        return [_view(v) for v in self._stub.ListInstances(req).instances]

    def cancel(self, instance_id: str, reason: str = "cancelled") -> None:
        self._stub.CancelInstance(pb.CancelInstanceRequest(instance_id=instance_id, reason=reason))

    def signal(self, instance_id: str, signal: str, payload: Any = None) -> None:
        """Deliver a named signal; ``payload`` merges into the instance context."""
        self._stub.SignalInstance(pb.SignalRequest(
            instance_id=instance_id, signal=signal, payload=to_value(payload)))

    # ---- schedules ----

    def create_schedule(self, workflow: str, *, every_s: Optional[float] = None,
                        cron: Optional[str] = None, context: Any = None) -> ScheduleView:
        if (every_s is None) == (cron is None):
            raise ValueError("pass exactly one of every_s or cron")
        req = pb.CreateScheduleRequest(workflow=workflow, context=to_value(context))
        if every_s is not None:
            req.every_millis = int(every_s * 1000)
        else:
            req.cron = cron
        return _schedule(self._stub.CreateSchedule(req))

    def list_schedules(self) -> list[ScheduleView]:
        return [_schedule(s) for s in self._stub.ListSchedules(pb.Empty()).schedules]

    def delete_schedule(self, schedule_id: str) -> None:
        self._stub.DeleteSchedule(pb.ScheduleIdRequest(id=schedule_id))

    # ---- cluster / health ----

    def health(self) -> dict:
        h = self._stub.HealthCheck(pb.Empty())
        return {"status": h.status, "node": h.node, "leader": h.leader}

    def cluster(self) -> dict:
        c = self._stub.GetCluster(pb.Empty())
        return {
            "self": c.self,
            "leader": c.leader,
            "members": [{"id": m.id, "name": m.name, "workers": m.workers,
                         "leader": m.leader, "alive": m.alive} for m in c.members],
        }

    # ---- low-level worker RPCs (used by Worker) ----

    def poll(self, worker_id: str, queues: Iterable[str], max_tasks: int,
             lease_millis: int, wait_millis: int) -> pb.TaskList:
        return self._stub.PollTasks(pb.PollRequest(
            worker_id=worker_id, queues=list(queues), max=max_tasks,
            lease_millis=lease_millis, wait_millis=wait_millis))

    def complete(self, task_id: str, lease_owner: str, result: Any) -> None:
        self._stub.CompleteTask(pb.TaskResultRequest(
            task_id=task_id, lease_owner=lease_owner, result=to_value(result)))

    def fail(self, task_id: str, lease_owner: str, message: str, retryable: bool) -> None:
        self._stub.FailTask(pb.TaskFailureRequest(
            task_id=task_id, lease_owner=lease_owner, message=message, retryable=retryable))

    def heartbeat(self, task_id: str, lease_owner: str, extend_millis: int) -> int:
        return self._stub.HeartbeatTask(pb.HeartbeatRequest(
            task_id=task_id, lease_owner=lease_owner, extend_millis=extend_millis)).lease_expires_at


def _view(v: pb.InstanceView) -> InstanceView:
    return InstanceView(
        id=v.id, workflow=v.workflow, version=v.version, status=v.status,
        termination_reason=v.termination_reason if v.HasField("termination_reason") else None,
        error=v.error if v.HasField("error") else None,
        context=from_value(v.context), created_at=v.created_at, updated_at=v.updated_at)


def _schedule(s: pb.ScheduleView) -> ScheduleView:
    return ScheduleView(s.id, s.workflow, s.every_millis, s.cron, s.next_fire_at, s.created_at)
