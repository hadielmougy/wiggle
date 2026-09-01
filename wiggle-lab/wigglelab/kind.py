"""kind cluster lifecycle + building/loading the wiggle image into it."""
from __future__ import annotations

from . import config as C
from . import shell


def context() -> str:
    return f"kind-{C.CLUSTER}"


def prereqs() -> dict[str, bool]:
    return {b: shell.have(b) for b in ("docker", "kind", "kubectl")}


def cluster_exists() -> bool:
    r = shell.run(["kind", "get", "clusters"])
    return r.ok and C.CLUSTER in r.out.split()


def create_cluster() -> shell.Result:
    if cluster_exists():
        return shell.Result(0, f"cluster '{C.CLUSTER}' already exists", "")
    return shell.run(["kind", "create", "cluster", "--name", C.CLUSTER], timeout=300)


def delete_cluster() -> shell.Result:
    return shell.run(["kind", "delete", "cluster", "--name", C.CLUSTER], timeout=180)


def image_exists_local() -> bool:
    return shell.run(["docker", "image", "inspect", C.IMAGE]).ok


def build_image_stream():
    """Yield build output lines; the last event is ('exit', code). Slow (compiles Java + dashboard)."""
    yield from shell.stream(["docker", "build", "-t", C.IMAGE, "."], cwd=C.REPO_ROOT)


def load_image() -> shell.Result:
    return shell.run(["kind", "load", "docker-image", C.IMAGE, "--name", C.CLUSTER], timeout=300)
