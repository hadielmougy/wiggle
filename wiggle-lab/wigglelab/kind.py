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


def node_container() -> str:
    return f"{C.CLUSTER}-control-plane"


def node_disk() -> str:
    """Disk on the kind node (all pods share it): bytes AND inodes. `No space left on device` during
    Postgres initdb can be either a full filesystem or exhausted inodes (many small files)."""
    node = node_container()
    blocks = []
    for label, flag in (("bytes", "-h"), ("inodes", "-ih")):
        r = shell.run(["docker", "exec", node, "df", flag, "/"], timeout=15)
        blocks.append(f"# {label}\n" + (r.out.strip() if r.ok else r.err.strip() or "(node not reachable)"))
    return "\n\n".join(blocks)


def host_docker_df() -> str:
    """`docker system df` on the host — images, containers, volumes, and build cache (repeated image
    builds accumulate cache here; reclaim with `docker builder prune -af`)."""
    r = shell.run(["docker", "system", "df"], timeout=15)
    return r.out if r.ok else (r.err.strip() or "(unavailable)")


def prune_node_images() -> shell.Result:
    """Remove images in the kind node not used by a running container (e.g. superseded wiggle:local
    loads). Safe: running pods keep their images; it just reclaims dangling/old layers on the node."""
    return shell.run(["docker", "exec", node_container(), "crictl", "rmi", "--prune"], timeout=180)
