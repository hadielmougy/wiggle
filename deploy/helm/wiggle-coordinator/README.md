# wiggle-coordinator Helm chart

Deploys the **Wiggle coordinator** — the cellular control plane that places namespaces onto cells and
resolves routing. It runs the same image as the server with `WIGGLE_ROLE=coordinator` (hardened,
non-root, distroless), and pairs with the [`wiggle`](../wiggle) chart (which deploys the cells).

## Store backends

Pick one with `store.backend`:

- **`jdbc`** (default, recommended for cloud) — the coordinator is **stateless**, so it runs as a
  `Deployment` you can scale; several replicas stay single-writer via a durable **leader lease** in the
  database. Point it at **its own small database** (never a tenant cell's).

  ```bash
  helm install coord deploy/helm/wiggle-coordinator \
    --set store.jdbc.url=jdbc:postgresql://coord-db:5432/wiggle_coord \
    --set store.jdbc.user=wiggle --set store.jdbc.password=secret
  # or point at an existing Secret (keys url/user/password):
  #   --set store.jdbc.existingSecret=coord-jdbc
  ```

- **`ratis`** — self-contained embedded Raft + RocksDB, no external database. Runs as a `StatefulSet`
  with a persistent volume for the data dir. This chart runs a **single member** (HA Ratis needs peer
  wiring it does not do — use the JDBC backend for HA).

  ```bash
  helm install coord deploy/helm/wiggle-coordinator \
    --set store.backend=ratis --set store.ratis.persistence.size=20Gi
  ```

## Wiring cells + clients to it

```
WIGGLE_COORDINATOR_URL=coord-wiggle-coordinator.<namespace>.svc:8099
```

Set that on the [`wiggle`](../wiggle) cell release (`env`) and on your workers/clients. The coordinator
has no HTTP health endpoint, so liveness/readiness are a TCP check on the gRPC port (8099).

## Defaults & hardening

Hardened by default (runs as uid 65532, read-only root filesystem with a writable `/tmp`, all
capabilities dropped, seccomp `RuntimeDefault`, service-account token off) — passes a *restricted*
PodSecurity namespace unmodified. Override `image.registry` to pull from an internal registry, or pin
`image.digest`.
