.PHONY: all test perf perf-track

PERF_INDEXES ?= 2
PERF_DAYS ?= 7
PERF_DOCS_PER_DAY ?= 5000
PERF_BATCH_SIZE ?= 500
PERF_SEARCH_REQUESTS ?= 64
PERF_SEARCH_CONCURRENCY ?= 8
PERF_HISTORY_DIR ?= .artifacts/perf
PERF_COMPARE_TO ?=

all:
	docker compose stop
	docker compose down
	docker compose build
	docker compose up

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run
gosec:
	go run github.com/securego/gosec/v2/cmd/gosec@latest ./...

test:
	go test -tags=integration ./... -cover
#	go test ./...
#	go test -tags=integration ./internal/server

perf:
	ELKGO_LOADTEST=1 \
	ELKGO_LOADTEST_INDEXES=$(PERF_INDEXES) \
	ELKGO_LOADTEST_DAYS=$(PERF_DAYS) \
	ELKGO_LOADTEST_DOCS_PER_DAY=$(PERF_DOCS_PER_DAY) \
	ELKGO_LOADTEST_BATCH_SIZE=$(PERF_BATCH_SIZE) \
	ELKGO_LOADTEST_SEARCH_REQUESTS=$(PERF_SEARCH_REQUESTS) \
	ELKGO_LOADTEST_SEARCH_CONCURRENCY=$(PERF_SEARCH_CONCURRENCY) \
	go test ./internal/server -run TestLoadProfile_RealCluster -count=1 -v

perf-track:
	ELKGO_LOADTEST=1 \
	ELKGO_LOADTEST_INDEXES=$(PERF_INDEXES) \
	ELKGO_LOADTEST_DAYS=$(PERF_DAYS) \
	ELKGO_LOADTEST_DOCS_PER_DAY=$(PERF_DOCS_PER_DAY) \
	ELKGO_LOADTEST_BATCH_SIZE=$(PERF_BATCH_SIZE) \
	ELKGO_LOADTEST_SEARCH_REQUESTS=$(PERF_SEARCH_REQUESTS) \
	ELKGO_LOADTEST_SEARCH_CONCURRENCY=$(PERF_SEARCH_CONCURRENCY) \
	ELKGO_LOADTEST_HISTORY_DIR=$(PERF_HISTORY_DIR) \
	ELKGO_LOADTEST_COMPARE_TO=$(PERF_COMPARE_TO) \
	go test ./internal/server -run TestLoadProfile_RealCluster -count=1 -v
