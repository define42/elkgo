# elkgo

`elkgo` is a small distributed search service written in Go. It uses:

- `etcd` for cluster membership and shard routing
- `Bleve` for local full-text indexing and search
- day-based partitioning plus per-day shard routing
- replica-aware fanout for indexing and query execution

It also ships with:

- a browser search UI at `/`
- a cluster dashboard at `/cluster`
- a separate `elkgo-testdata` app for loading demo data into the cluster

## What It Does

- indexes JSON documents into day-partitioned Bleve shards
- distributes each day across `48` shards
- replicates each shard to multiple nodes
- searches across one day or a day range
- merges top hits across shards
- exposes cluster state, shard placement, and per-shard event counts

## Data Model

Each document must contain:

- `id`: non-empty string
- one parseable timestamp field:
  - `timestamp`
  - `event_time`
  - `created`
  - `ts`
  - `@timestamp`

The timestamp decides the UTC partition day. The service also adds:

- `partition_day`

## Generic JSON Support

Newly created indexes use Bleve dynamic mapping for generic JSON documents.

- `id` is explicitly mapped as a keyword field
- `partition_day` is explicitly mapped as a keyword field
- all other fields are inferred dynamically by Bleve

That means Bleve will normally treat:

- strings as text, unless they parse as dates
- numbers as numeric
- booleans as boolean
- nested objects and arrays recursively

Important note:

- existing on-disk Bleve indexes keep the mapping they were created with
- if you changed mapping behavior and want a clean start, recreate the data volumes

Example reset:

```bash
docker compose down -v
docker compose up --build
```

## Architecture

At a high level:

1. Nodes register themselves in `etcd`.
2. A bootstrap request creates shard routing for an `index + day`.
3. Documents are routed to a shard by hashing `id`.
4. The primary replica writes locally and replicates to the remaining replicas.
5. Searches fan out to one healthy replica per shard and merge the results.

## Web UI

- `/`: search page
- `/cluster`: cluster dashboard

The search page supports:

- index selection
- `day_from` and `day_to`
- free-text Bleve query syntax
- blank query for match-all

The cluster dashboard shows:

- registered nodes
- shard placement
- primary/replica ownership
- event counts per shard

## Running With Docker Compose

The repository includes a `docker-compose.yml` that starts:

- `etcd`
- `elkgo1`
- `elkgo2`
- `elkgo3`
- `elkgo4`
- `elkgo-testdata`

Start everything:

```bash
docker compose up --build
```

Access:

- search UI: `http://127.0.0.1:8081/`
- cluster dashboard: `http://127.0.0.1:8081/cluster`

Watch test data loading:

```bash
docker compose logs -f elkgo-testdata
```

The test-data generator logs progress every `1000` ingested events.

## Docker Images

The `Dockerfile` builds two scratch-based targets:

- main server image
- `testdata` generator image

Build the server image:

```bash
docker build -t elkgo .
```

Build the test-data generator image:

```bash
docker build --target testdata -t elkgo-testdata .
```

## Running A Single Node Locally

You need an `etcd` endpoint first.

Example:

```bash
go run . \
  -mode=both \
  -node-id=n1 \
  -listen=:8081 \
  -data=./data \
  -etcd-endpoints=http://127.0.0.1:2379
```

Useful flags:

- `-mode=node|coordinator|both`
- `-node-id`
- `-listen`
- `-public-addr`
- `-data`
- `-etcd-endpoints`
- `-replication-factor`

## HTTP API

### Health

```http
GET /healthz
```

Returns basic node and membership info.

### Bootstrap Routing

```http
POST /admin/bootstrap?index=events&day=2026-03-21&replication_factor=3
```

Creates routing for a specific `index + day`.

### List Available Indexes

```http
GET /admin/indexes
```

Returns known indexes and the days that currently have routing.

### Routing And Cluster State

```http
GET /admin/routing
GET /admin/routing?stats=1
GET /admin/routing?index=events&day=2026-03-21&shard=7
```

With `stats=1`, shard event counts are included.

### Index One Document

```http
POST /index?index=events
Content-Type: application/json
```

Example:

```json
{
  "id": "evt-1",
  "timestamp": "2026-03-21T12:00:00Z",
  "service": "api",
  "message": "timeout talking to etcd",
  "latency_ms": 125
}
```

### Bulk Ingest

```http
POST /bulk?index=events
Content-Type: application/x-ndjson
```

Body format:

- one JSON document per line
- newline-delimited JSON

Example:

```bash
curl -XPOST 'http://127.0.0.1:8081/bulk?index=events' \
  -H 'content-type: application/x-ndjson' \
  --data-binary $'{"id":"evt-1","timestamp":"2026-03-21T10:00:00Z","message":"first"}\n{"id":"evt-2","timestamp":"2026-03-21T10:01:00Z","message":"second"}\n'
```

### Search

```http
GET /search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=timeout&k=10
GET /search?index=events&day_from=2026-03-15&day_to=2026-03-21&q=service:api
```

Rules:

- `index` is required
- both `day_from` and `day_to` are required
- for a single day search, set `day_from` and `day_to` to the same date
- `q` may be empty for match-all
- `k` defaults to `10`

## Bleve Search Syntax Supported By This App

This app passes the `q` parameter directly to:

- `bleve.NewQueryStringQuery(q)`

If `q` is empty, it uses:

- `bleve.NewMatchAllQuery()`

That means the supported search syntax is Bleve query-string syntax.

### Supported Query Styles

- free text:
  - `timeout`
- phrase search:
  - `"api timeout"`
- fielded search:
  - `service:api`
  - `message:timeout`
  - `id:evt-00001`
- required and excluded terms:
  - `+error -debug`
- boost:
  - `timeout^3`
- fuzzy:
  - `watex~`
  - `watex~2`
- wildcard:
  - `mart*`
- regexp:
  - `/mar.*ty/`
  - `service:/ap.*/`
- numeric comparisons:
  - `latency_ms:>100`
  - `latency_ms:>=100`
  - `score:<80`
- date comparisons:
  - `timestamp:>="2026-03-21T00:00:00Z"`
  - `observed_at:<"2026-03-22T00:00:00Z"`
- exact-ish keyword field lookups on mapped keyword fields:
  - `id:evt-123`
  - `partition_day:2026-03-21`
- blank query:
  - match all documents in the requested day scope

### Search Examples

All documents for a day:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21'
```

Phrase search:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=%22api%20timeout%22'
```

Fielded search:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=service:api'
```

Required and excluded:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=%2Berror%20-debug'
```

Numeric filter:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=latency_ms:%3E%3D100'
```

Date filter:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-21&day_to=2026-03-21&q=observed_at:%3E%3D%222026-03-21T00:00:00Z%22'
```

Across a day range:

```bash
curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-15&day_to=2026-03-21&q=service:api'
```

### Notes About Search Semantics

- day scoping is handled by the API parameters, not by the Bleve query string
- search fans out to one healthy replica per shard
- the coordinator merges hits and sorts by Bleve score
- generic field typing depends on how the JSON was decoded and how Bleve infers the field
- parseable date strings may be indexed as datetimes instead of plain text

## Test Data Generator

`elkgo-testdata` is a separate app that:

- waits for the cluster to be ready
- elects one leader via `etcd`
- bootstraps the last `7` UTC days
- ingests `70,000` demo events total
- ingests `10,000` events per day
- writes completion markers to `etcd`

It logs:

- day start
- progress every `1000` events
- final completion

You can also run it directly:

```bash
go run ./cmd/elkgo-testdata \
  -server-url=http://127.0.0.1:8081 \
  -etcd-endpoints=http://127.0.0.1:2379 \
  -replication-factor=3
```

## Development

Format:

```bash
gofmt -w main.go cmd/elkgo-testdata/main.go internal/server/*.go internal/testdatagen/*.go internal/webui/*.go
```

Unit tests:

```bash
go test ./...
```

Integration test with Docker and Testcontainers:

```bash
go test -tags=integration ./internal/server -run TestIntegration_ClusterDashboardAndSearch -v
```

## Project Layout

```text
main.go
cmd/elkgo-testdata/
internal/server/
internal/testdatagen/
internal/webui/
Dockerfile
docker-compose.yml
```

## Caveats

- routing must exist before documents can be indexed or searched
- data is partitioned by UTC day
- existing Bleve shard data keeps its original mapping
- query syntax is Bleve query-string syntax, not Elasticsearch DSL or SQL
- internal endpoints under `/internal/*` are node-to-node implementation details
