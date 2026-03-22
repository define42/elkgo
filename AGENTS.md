# AGENTS.md

This file gives coding agents a fast, repo-specific guide for working in `elkgo`.

## Project Summary

`elkgo` is a distributed search service written in Go. It uses:

- `etcd` for membership, routing, and distributed coordination
- `Bleve` for local indexing and search
- day-based partitions plus per-day shard routing
- replica-aware indexing, search, repair, and rebalance flows

The repository also includes:

- a browser search UI at `/`
- a cluster dashboard at `/cluster`
- `elkgo-testdata` for loading demo or perf data

## Repository Map

- `main.go`
  Runs the main `elkgo` server binary.
- `cmd/elkgo-testdata`
  CLI for generating and ingesting test data.
- `internal/server`
  Core cluster, routing, ingest, search, repair, rebalance, retention, transport, and storage logic.
- `internal/webui`
  Static HTML pages for the search UI and cluster dashboard.
- `internal/testdatagen`
  Shared generator code used by `elkgo-testdata`.
- `docker-compose.yml`
  Local multi-node demo cluster with etcd and test-data seeding.
- `makefile`
  Common local commands, including perf runs.

## Important Architecture Notes

- Data is partitioned by UTC day.
- Shard placement is coordinated through etcd.
- All ingested data must remain indexed. Do not change the system to index only a subset of ingested fields unless the task explicitly changes that product requirement.
- The product must not require a predefined schema for ingest.
- The system must support dynamically structured JSON documents.
- Users must be able to search across all ingested fields.
- Searches use `day_from` and `day_to`; do not reintroduce a standalone `day` search parameter.
- Search supports all indexes. Treat `_all`, `*`, `all`, or omitted `index` consistently if touching the search path.
- Search is performance-sensitive and currently uses a two-phase flow:
  1. shard-local top-hit collection
  2. global top-K merge
  3. source fetch only for final winners
- Replica repair should stay snapshot-first when possible, with streamed doc replay only as fallback.
- Read paths should remain non-mutating. Opening a shard for search or inspection must not create a new index on disk.
- Retention is per index and removes expired routed days and local shard data.

## Working Agreements

- Keep changes small and targeted when possible.
- Preserve existing API behavior unless the task explicitly changes it.
- Prefer real tests over mocks.
  Use `httptest`, temporary directories, embedded etcd, and real Bleve indexes where practical.
- When touching Go files, run `gofmt -w` on the files you changed.
- Do not add unnecessary dependencies.
- Avoid destructive git operations that discard user work.

## Testing Expectations

Test coverage must stay above `80%`.

For most code changes:

```bash
go test ./...
```

For focused server work:

```bash
go test ./internal/server
```

For integration-heavy coverage:

```bash
go test -tags=integration ./...
```

When a task changes code in a meaningful way, verify coverage with:

```bash
go test ./... -coverprofile=/tmp/elkgo.cover
go tool cover -func=/tmp/elkgo.cover | tail -n 1
```

For perf/load checks:

```bash
make perf
make perf-track
```

Useful perf overrides:

```bash
make perf PERF_DAYS=1 PERF_DOCS_PER_DAY=100 PERF_BATCH_SIZE=50
make perf-track PERF_HISTORY_DIR=.artifacts/perf
```

## Change Areas To Treat Carefully

### Search

- `internal/server/search.go`
- `internal/server/routing.go`
- `internal/server/transport.go`

Watch for:

- shard fanout explosion
- unnecessary full-document fetches
- cloning or scanning full routing state on hot paths
- unbounded goroutine creation

### Ingest and Replication

- `internal/server/ingest.go`
- `internal/server/repair.go`
- `internal/server/shard_transfer.go`
- `internal/server/snapshot_transfer.go`

Watch for:

- extra JSON decode/encode hops
- large fully buffered request bodies
- slow follower catch-up paths
- write quorum regressions

### Storage

- `internal/server/storage.go`

Watch for:

- mutating read paths
- leaked Bleve handles
- unbounded open-index growth
- non-atomic shard replacement

### Cluster Coordination

- `internal/server/cluster.go`
- `internal/server/rebalance.go`
- `internal/server/retention.go`

Watch for:

- background goroutines using the wrong context
- stale routing caches
- expensive full-state reloads on hot watch paths
- rebalance concurrency regressions

## UI Notes

- Search UI is in `internal/webui/home.go`.
- Cluster dashboard is in `internal/webui/cluster.go`.
- Keep UI changes aligned with server API behavior.
- The search UI should lean toward a Kibana Discover style in dark mode: dense and app-like rather than marketing-like, with compact toolbars, a field/sidebar workflow, histogram-driven search context, and a tabular event stream.
- Avoid drifting the search UI back toward oversized rounded-card layouts or explanatory hero copy when refining the main discover/search experience.
- In the search UI, the available field list must be derived dynamically from the fields present in the current result data rather than from a hardcoded or artificially capped subset.
- The cluster dashboard already exposes retention and index size; avoid drifting UI labels from backend field names.

## Local Development Commands

Start the demo cluster:

```bash
docker compose up --build
```

Run the default test suite from the makefile:

```bash
make test
```

## When Adding Tests

- Prefer end-to-end behavior over isolated internals when reasonable.
- Reuse existing test helpers in `internal/server/server_test.go` and related test files before creating new patterns.
- If a test depends on time or date boundaries, keep UTC handling explicit to avoid midnight flakes.
- If a test reserves ports, be mindful of TOCTOU races.

## Output Expectations For Agents

- Summarize user-visible behavior changes, not just file edits.
- Call out any remaining risk honestly if a deeper architectural limit still exists.
- If you could not run relevant verification, say so clearly.
