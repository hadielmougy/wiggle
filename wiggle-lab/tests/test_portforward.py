"""Regression: PortForwards.ensure() must return a grpc-ready 'host:port' STRING, not the bare int
port. Passing the int to grpc.insecure_channel raised `AttributeError: 'int' object has no attribute
'encode'` on the first gRPC call (open_epoch). Run: python -m pytest  (or python tests/test_portforward.py)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wigglelab import portforward as pf


class _FakeProc:
    def poll(self):
        return None

    def terminate(self):
        pass

    def wait(self, timeout=None):
        pass


def _patched_manager(monkeypatch=None):
    pf._port_open = lambda port, host="127.0.0.1", timeout=0.4: True
    pf.subprocess.Popen = lambda *a, **k: _FakeProc()
    return pf.PortForwards()


def test_ensure_returns_grpc_target_string():
    m = _patched_manager()
    target = m.ensure("coordinator", "coordinator", 8099, 18099)
    assert target == "127.0.0.1:18099"
    assert isinstance(target, str)

    # idempotent call returns the same string
    assert m.ensure("coordinator", "coordinator", 8099, 18099) == "127.0.0.1:18099"

    # the target is directly usable by grpc (the operation that regressed)
    import grpc
    grpc.insecure_channel(target).close()


if __name__ == "__main__":
    test_ensure_returns_grpc_target_string()
    print("ok")
