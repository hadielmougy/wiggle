# GCP ceiling environment

`ceiling.sh` sets up a temporary GCP environment, deploys your checkout to it and runs
`RateCeilingBench` to find the highest start rate a single server can sustain against a managed
PostgreSQL.

```
Cloud SQL PostgreSQL 16 ── private IP ── server VM ──┬── gRPC :8080 ── worker VM (N WorkerMain processes)
(db-custom-8-32768, 250GB SSD)           (wiggle server) └── gRPC :8080 ── load VM   (RateCeilingBench)
```

Workers and the load generator run on separate VMs, so neither takes CPU from the other.

The database and all three VMs sit in one zone and VPC, so network round trips are sub-millisecond. The VMs
are reached over IAP SSH and have no open ingress apart from traffic inside the VPC.

## Usage

```bash
export GCP_PROJECT=my-sandbox-project    # required; the gcloud default project is ignored on purpose
deploy/gcp/ceiling.sh up                 # VPC, Cloud SQL, 3 VMs  (~10-15 min)
deploy/gcp/ceiling.sh deploy             # git archive $REF -> build on each VM -> start the server
deploy/gcp/ceiling.sh run                # workers + ceiling ladder -> deploy/gcp/results/<timestamp>/
deploy/gcp/ceiling.sh down               # delete everything
```

Other commands: `status`, `ssh server|worker|load`, `logs server|worker|load`, and `collect <timestamp>`. Use
`collect` to fetch results if the SSH session dropped during a run. The bench runs as a systemd
unit, so it keeps going without the session.

`deploy` ships only **committed** code (`REF`, default `HEAD`). To compare revisions, run
`REF=<sha> ceiling.sh deploy && ceiling.sh run` once per revision.

## What a run collects

| file | content |
|---|---|
| `params.env` | revision, machine types, DB tier and every knob used |
| `bench.log` | per-stage target vs achieved rate, submit p50/p99, probe sojourn, verdict, ceiling |
| `pg_stat_statements.txt` | top 25 statements by total time during the run |
| `server-mpstat.txt`, `worker-mpstat.txt`, `load-mpstat.txt` | per-5s CPU on each VM (shows whether workers or the generator became the limit) |
| `server.jfr` | server flight recording (`settings=profile`, last hour) |
| `server-journal.txt`, `worker-journal.txt` | server and worker logs |

For Cloud SQL CPU, IOPS and WAL, use System Insights in the console. The run prints the link.

## Knobs (environment variables)

| var | default | |
|---|---|---|
| `REGION` / `ZONE` | `us-central1` / `-a` | |
| `PREFIX` | `wiggle-ceiling` | name prefix; lets several environments coexist |
| `DB_TIER` | `db-custom-8-32768` | 8 vCPU / 32 GB |
| `DB_DISK_GB` | `250` | Cloud SQL SSD IOPS scale with size |
| `DB_HA` | `false` | `true` = regional (synchronous standby; adds commit latency) |
| `DB_MAX_CONNECTIONS` | `500` | |
| `SERVER_MACHINE` | see below | |
| `WORKER_MACHINE` | see below | runs the `WORKERS` worker processes |
| `LOAD_MACHINE` | see below | runs RateCeilingBench |
| `SERVER_POOL` | `128` | `WIGGLE_JDBC_POOL_SIZE` |
| `SERVER_ENV` | | extra server env, space-separated `K=V` (e.g. `WIGGLE_RECORD_STEP_IO=false`) |
| `WORKERS` × `WORKER_CONCURRENCY` | `4` × `256` | total worker slots must cover rate × sojourn |
| `BENCH_RATES` | `500,1000,2000,3000,4000,5000,7500,10000,15000,20000` | the ladder stops at the first failing stage |
| `BENCH_STAGE_SECONDS` / `BENCH_CONFIRM_SECONDS` | `30` / `60` | |
| `BENCH_THREADS` | `256` | submitter threads |

Every VM defaults to `c3-standard-8`, so runs stay comparable. Each `*_MACHINE` also accepts a
comma-separated fallback list, tried in order when a zone is out of capacity (for example
`n2-standard-8,c3-standard-8`). `params.env` records the type each VM actually got.

Machine, tier and disk settings take effect at `up`. Server settings take effect at `deploy`.
Bench settings take effect at `run`.

## Reading the result

The ceiling is only meaningful if the system under test is the bottleneck. Before trusting the
number, check two things:

- **`worker-mpstat.txt` and `load-mpstat.txt` should not be pegged.** If the worker VM is, raise
  `WORKER_MACHINE`; if the load VM is, raise `LOAD_MACHINE`. If neither is busy but probes
  queue up, raise `WORKERS`/`WORKER_CONCURRENCY` (slot starvation).
- **Achieved rate should track the target** in passing stages. If it doesn't, raise
  `BENCH_THREADS`.

After that, compare server CPU (`server-mpstat.txt`, JFR) against Cloud SQL CPU and IOPS
(System Insights) to see which side saturates first.

All of this costs money while it exists: an 8 vCPU Cloud SQL instance plus three 8 vCPU VMs.
Run `down` when you are done.
