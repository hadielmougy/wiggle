# Cell Coordinator — implementation tickets

Phased, dependency-ordered tickets for the optional cell coordinator (sharding a workflow's instances
across isolated cells). Each ticket is written against the actual files it touches, with acceptance
criteria and requirement tags (`R#`) that point back to the design reference.

**Guiding rule:** every Phase 0 task defaults to today's behaviour — `WIGGLE_ROLE=cell` and no
`WIGGLE_COORDINATOR_URL` means a standalone deployment is byte-for-byte unchanged. The coordinator is
built as a *role* beside the cell, then cells adopt it.

| Phase | Theme | Tickets |
|---|---|---|
| [0](phase-0-tickets.md) | Non-breaking seams (default = today) | T1 role switch · T2 schema layering · T3 dormant client hooks |
| [1](phase-1-tickets.md) | Coordinator MVP (one namespace/epoch/cell) | T4 proto · T5 `coord_*` store · T6 bundle+reconcile · T7 node lifecycle |
| [2](phase-2-tickets.md) | Routing & polyglot SDK | T8 epoch-aware id · T9 Resolve/ActiveCells · T10 SDK resolver ×3 |
| [3](phase-3-tickets.md) | Definitions, scale-out, provisioning | T11 definition fan-out · T12 epochs/drain/retire · T13 CellDeployer |
| [4](phase-4-tickets.md) | Later (rare/advanced) | T14 region placement · T15 adopt a running cell · T16 live-migration saga |

**Start here:** T1 is the keystone — a pure, non-breaking refactor of `WiggleServer` that makes the
coordinator role possible. Ship Phase 0 (T1–T3) as three PRs, then you have a real coordinator by the
end of Phase 1 and the first observable capability (routing/scale-out) in Phases 2–3.

**Open design decision:** the routing hash key (instance id vs business key, R17) is deferred and
per-workflow; the default is hashing the id. It changes only `placeNew` and whether by-key lookup is
directory-free — not the id format or `route(id)` — so it doesn't block any ticket.
