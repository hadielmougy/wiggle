"""A thin client for a cell's WiggleControlPlane gRPC service (data plane: start/observe)."""
from __future__ import annotations

import grpc

from .pb import wiggle_pb2 as wpb
from .pb import wiggle_pb2_grpc as wgrpc
from .pbjson import msg_to_dict, to_struct, to_value


class CellClient:
    def __init__(self, target: str):
        self.target = target
        self._chan = grpc.insecure_channel(target)
        self._stub = wgrpc.WiggleControlPlaneStub(self._chan)

    def close(self):
        self._chan.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def health(self) -> dict:
        return msg_to_dict(self._stub.HealthCheck(wpb.Empty(), timeout=5))

    def cluster(self) -> dict:
        return msg_to_dict(self._stub.GetCluster(wpb.Empty(), timeout=5))

    def register_workflow(self, definition: dict) -> dict:
        req = wpb.WorkflowDefinition(definition=to_struct(definition))
        return msg_to_dict(self._stub.RegisterWorkflow(req))

    def list_workflow_names(self) -> list[str]:
        return list(self._stub.ListWorkflows(wpb.Empty(), timeout=5).workflows)

    def start_instance(self, workflow: str, context: dict | None = None,
                       version: int | None = None, correlation_id: str | None = None) -> str:
        req = wpb.StartInstanceRequest(workflow=workflow, context=to_value(context or {}))
        if version is not None:
            req.version = version
        if correlation_id:
            req.correlation_id = correlation_id
        return self._stub.StartInstance(req, timeout=10).instance_id

    def list_instances(self, workflow: str | None = None, status: str | None = None,
                       limit: int = 200) -> list[dict]:
        req = wpb.ListInstancesRequest(limit=limit)
        if workflow is not None:
            req.workflow = workflow
        if status is not None:
            req.status = status
        resp = self._stub.ListInstances(req, timeout=10)
        return [msg_to_dict(i) for i in resp.instances]

    def get_instance(self, instance_id: str) -> dict:
        return msg_to_dict(self._stub.GetInstance(wpb.InstanceIdRequest(instance_id=instance_id), timeout=5))

    def cancel_instance(self, instance_id: str, reason: str = "lab") -> str:
        resp = self._stub.CancelInstance(
            wpb.CancelInstanceRequest(instance_id=instance_id, reason=reason), timeout=5)
        return resp.cancelled
