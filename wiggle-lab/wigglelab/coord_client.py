"""A thin client for the coordinator's CellCoordinator gRPC service (control plane)."""
from __future__ import annotations

import json

import grpc

from .pb import coordinator_pb2 as cpb
from .pb import coordinator_pb2_grpc as cgrpc
from .pbjson import msg_to_dict


class CoordinatorClient:
    def __init__(self, target: str):
        self.target = target
        self._chan = grpc.insecure_channel(target)
        self._stub = cgrpc.CellCoordinatorStub(self._chan)

    def close(self):
        self._chan.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    # ---- admin / control ----

    def open_epoch(self, namespace: str, ring: list[tuple[int, str, str]]) -> dict:
        """Open a new epoch publishing a shard->cell ring. ``ring`` is [(shard, cell_id, region)].
        Returns the resulting Policy (current_epoch, revision, epochs)."""
        slots = [cpb.RingSlot(shard=s, cell_id=c, region=r or "") for (s, c, r) in ring]
        policy = self._stub.OpenEpoch(cpb.OpenEpochRequest(namespace=namespace, ring=slots))
        return msg_to_dict(policy)

    def register_workflow(self, namespace: str, name: str, definition: dict) -> dict:
        """Fan a workflow definition out to every cell of the namespace (allocate)."""
        req = cpb.RegisterWorkflowRequest(
            namespace=namespace, name=name,
            definition=json.dumps(definition).encode("utf-8"),
        )
        return msg_to_dict(self._stub.RegisterWorkflow(req))

    def deregister_workflow(self, namespace: str, name: str) -> bool:
        resp = self._stub.DeregisterWorkflow(cpb.DeregisterWorkflowRequest(namespace=namespace, name=name))
        return resp.removed

    def list_workflows(self, namespace: str) -> list[dict]:
        resp = self._stub.ListWorkflows(cpb.ListWorkflowsRequest(namespace=namespace))
        return [msg_to_dict(w) for w in resp.workflows]

    # ---- resolution (read) ----

    def resolve_namespace(self, namespace: str, region: str = "") -> dict:
        resp = self._stub.Resolve(cpb.ResolveRequest(namespace=namespace, caller_region=region))
        return msg_to_dict(resp)

    def active_cells(self, namespace: str, region: str = "") -> dict:
        resp = self._stub.ActiveCells(cpb.ActiveCellsRequest(namespace=namespace, caller_region=region))
        return msg_to_dict(resp)

    def health(self) -> bool:
        """Best-effort reachability probe via a cheap Resolve (namespaces without a ring throw)."""
        try:
            self._stub.ActiveCells(cpb.ActiveCellsRequest(namespace="__probe__"), timeout=3)
            return True
        except grpc.RpcError as e:
            # UNAVAILABLE means we couldn't reach it; anything else means the server answered.
            return e.code() != grpc.StatusCode.UNAVAILABLE
