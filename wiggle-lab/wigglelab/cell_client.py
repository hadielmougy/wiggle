"""A thin client for a cell's control-plane API over HTTP + JSON (data plane: start/observe)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request


class ApiError(RuntimeError):
    """A call the server answered with a non-2xx status."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code


class CellClient:
    def __init__(self, target: str):
        self.target = target
        self._base = target if "://" in target else f"http://{target}"

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def _call(self, method: str, body: dict | None = None, timeout: float | None = None) -> dict:
        req = urllib.request.Request(f"{self._base}/v1/{method}", data=json.dumps(body or {}).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read().decode())
            except ValueError:
                err = {}
            raise ApiError(e.code, err.get("code", "UNKNOWN"), err.get("message", str(e))) from None
        return json.loads(text) if text else {}

    def health(self) -> dict:
        return self._call("HealthCheck", timeout=5)

    def cluster(self) -> dict:
        return self._call("GetCluster", timeout=5)

    def register_workflow(self, definition: dict) -> dict:
        return self._call("RegisterWorkflow", {"definition": definition})

    def list_workflow_names(self) -> list[str]:
        return list(self._call("ListWorkflows", timeout=5).get("workflows", []))

    def start_instance(self, workflow: str, context: dict | None = None,
                       version: int | None = None, correlation_id: str | None = None) -> str:
        req = {"workflow": workflow, "context": context or {}}
        if version is not None:
            req["version"] = version
        if correlation_id:
            req["correlationId"] = correlation_id
        return self._call("StartInstance", req, timeout=10)["instanceId"]

    def list_instances(self, workflow: str | None = None, status: str | None = None,
                       limit: int = 200) -> list[dict]:
        req: dict = {"limit": limit}
        if workflow is not None:
            req["workflow"] = workflow
        if status is not None:
            req["status"] = status
        return list(self._call("ListInstances", req, timeout=10).get("instances", []))

    def get_instance(self, instance_id: str) -> dict:
        return self._call("GetInstance", {"instanceId": instance_id}, timeout=5)

    def cancel_instance(self, instance_id: str, reason: str = "lab") -> str:
        return self._call("CancelInstance", {"instanceId": instance_id, "reason": reason}, timeout=5)["cancelled"]
