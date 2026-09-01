"""Manages background `kubectl port-forward` processes so the host-side lab can reach in-cluster
gRPC services (the coordinator and each cell)."""
from __future__ import annotations

import socket
import subprocess
import time

from . import config as C
from . import kind


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class PortForwards:
    """Keyed registry of live port-forwards. Not thread-safe; the Streamlit app uses it single-threaded."""

    def __init__(self):
        self._procs: dict[str, tuple[subprocess.Popen, int]] = {}

    def ensure(self, key: str, svc: str, remote_port: int, local_port: int, wait: float = 10.0) -> str:
        """Ensure a forward svc/<svc>:remote -> 127.0.0.1:local is live; returns the gRPC target
        string ``127.0.0.1:<local_port>`` (ready to hand to grpc.insecure_channel)."""
        existing = self._procs.get(key)
        if existing and existing[0].poll() is None and _port_open(existing[1]):
            return f"127.0.0.1:{existing[1]}"
        if existing:
            self.stop(key)
        proc = subprocess.Popen(
            ["kubectl", "--context", kind.context(), "-n", C.K8S_NAMESPACE,
             "port-forward", f"svc/{svc}", f"{local_port}:{remote_port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self._procs[key] = (proc, local_port)
        deadline = time.time() + wait
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"port-forward for {svc} exited early (is the pod ready?)")
            if _port_open(local_port):
                return f"127.0.0.1:{local_port}"
            time.sleep(0.25)
        raise TimeoutError(f"port-forward to {svc}:{remote_port} not ready on :{local_port}")

    def target(self, key: str) -> str | None:
        p = self._procs.get(key)
        if p and p[0].poll() is None and _port_open(p[1]):
            return f"127.0.0.1:{p[1]}"
        return None

    def stop(self, key: str):
        p = self._procs.pop(key, None)
        if p:
            p[0].terminate()
            try:
                p[0].wait(timeout=3)
            except subprocess.TimeoutExpired:
                p[0].kill()

    def stop_all(self):
        for key in list(self._procs):
            self.stop(key)
