package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandleSearch_BlankQueryAcrossDayRangeReturnsAllDocuments(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day1 := "2026-03-20"
	day2 := "2026-03-21"

	setTestRoute(s, "events", day1, 0, []string{"n1"})
	setTestRoute(s, "events", day2, 0, []string{"n1"})

	indexTestDocument(t, s, "events", day1, 0, "doc-a", Document{
		"id":        "doc-a",
		"timestamp": day1 + "T10:00:00Z",
		"title":     "First document",
		"message":   "alpha event",
	})
	indexTestDocument(t, s, "events", day2, 0, "doc-b", Document{
		"id":        "doc-b",
		"timestamp": day2 + "T11:00:00Z",
		"title":     "Second document",
		"message":   "beta event",
	})

	searchURL := ts.URL + "/search?index=events&day_from=" + url.QueryEscape(day1) + "&day_to=" + url.QueryEscape(day2) + "&k=10"
	resp, err := http.Get(searchURL)
	if err != nil {
		t.Fatalf("search request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Index         string     `json:"index"`
		Days          []string   `json:"days"`
		Query         string     `json:"query"`
		K             int        `json:"k"`
		Hits          []ShardHit `json:"hits"`
		PartialErrors []string   `json:"partial_errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload.Index != "events" {
		t.Fatalf("expected index events, got %q", payload.Index)
	}
	if payload.Query != "" {
		t.Fatalf("expected blank query, got %q", payload.Query)
	}
	if payload.K != 10 {
		t.Fatalf("expected k=10, got %d", payload.K)
	}
	if !reflect.DeepEqual(payload.Days, []string{day1, day2}) {
		t.Fatalf("unexpected days: %#v", payload.Days)
	}
	if len(payload.PartialErrors) != 0 {
		t.Fatalf("expected no partial errors, got %#v", payload.PartialErrors)
	}
	if len(payload.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(payload.Hits))
	}

	docIDs := []string{payload.Hits[0].DocID, payload.Hits[1].DocID}
	sort.Strings(docIDs)
	if !reflect.DeepEqual(docIDs, []string{"doc-a", "doc-b"}) {
		t.Fatalf("unexpected doc ids: %#v", docIDs)
	}
}

func TestHandleBulkIngest_NDJSONIndexesDocuments(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	setTestRoute(s, "events", day, keyToShard("bulk-a", enforcedShardsPerDay), []string{"n1"})
	setTestRoute(s, "events", day, keyToShard("bulk-b", enforcedShardsPerDay), []string{"n1"})

	body := strings.Join([]string{
		`{"id":"bulk-a","timestamp":"2026-03-21T09:00:00Z","title":"Bulk A","message":"first bulk event"}`,
		`{"id":"bulk-b","timestamp":"2026-03-21T09:05:00Z","title":"Bulk B","message":"second bulk event"}`,
	}, "\n") + "\n"

	resp, err := http.Post(ts.URL+"/bulk?index=events", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatalf("bulk request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var bulkPayload struct {
		OK      bool     `json:"ok"`
		Index   string   `json:"index"`
		Indexed int      `json:"indexed"`
		Failed  int      `json:"failed"`
		Errors  []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bulkPayload); err != nil {
		t.Fatalf("decode bulk response: %v", err)
	}

	if !bulkPayload.OK || bulkPayload.Index != "events" || bulkPayload.Indexed != 2 || bulkPayload.Failed != 0 {
		t.Fatalf("unexpected bulk payload: %#v", bulkPayload)
	}
	if len(bulkPayload.Errors) != 0 {
		t.Fatalf("expected no bulk errors, got %#v", bulkPayload.Errors)
	}

	searchResp, err := http.Get(ts.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&k=10")
	if err != nil {
		t.Fatalf("search request failed: %v", err)
	}
	defer searchResp.Body.Close()

	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected search status 200, got %d", searchResp.StatusCode)
	}

	var searchPayload struct {
		Hits []ShardHit `json:"hits"`
	}
	if err := json.NewDecoder(searchResp.Body).Decode(&searchPayload); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if len(searchPayload.Hits) != 2 {
		t.Fatalf("expected 2 search hits, got %d", len(searchPayload.Hits))
	}
}

func TestHandleBulkIngest_BatchesReplicaWritesByShard(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 7
	docID1 := findDocIDForShard(t, shardID, "replica-bulk-a")
	docID2 := findDocIDForShard(t, shardID, "replica-bulk-b")

	replicaCalls := 0
	replicaItems := 0
	replicaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/index_batch" {
			t.Fatalf("unexpected replica path: %s", r.URL.Path)
		}
		var req internalIndexBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode replica batch request: %v", err)
		}
		if req.Replicate {
			t.Fatalf("expected replica batch request to skip further replication")
		}
		if req.ShardID != shardID {
			t.Fatalf("expected shard %d, got %d", shardID, req.ShardID)
		}
		replicaCalls++
		replicaItems += len(req.Items)
		writeJSON(w, http.StatusOK, internalIndexBatchResponse{
			OK:      true,
			Index:   req.IndexName,
			Day:     req.Day,
			Shard:   req.ShardID,
			Indexed: len(req.Items),
		})
	}))
	defer replicaServer.Close()

	s.membersMu.Lock()
	s.members["n2"] = NodeInfo{ID: "n2", Addr: replicaServer.URL}
	s.membersMu.Unlock()

	setTestRoute(s, "events", day, shardID, []string{"n1", "n2"})

	body := strings.Join([]string{
		fmt.Sprintf(`{"id":"%s","timestamp":"2026-03-21T09:00:00Z","title":"Bulk A","message":"first replica batch event"}`, docID1),
		fmt.Sprintf(`{"id":"%s","timestamp":"2026-03-21T09:05:00Z","title":"Bulk B","message":"second replica batch event"}`, docID2),
	}, "\n") + "\n"

	resp, err := http.Post(ts.URL+"/bulk?index=events", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatalf("bulk request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var bulkPayload struct {
		OK      bool     `json:"ok"`
		Indexed int      `json:"indexed"`
		Failed  int      `json:"failed"`
		Errors  []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bulkPayload); err != nil {
		t.Fatalf("decode bulk response: %v", err)
	}
	if !bulkPayload.OK || bulkPayload.Indexed != 2 || bulkPayload.Failed != 0 {
		t.Fatalf("unexpected bulk payload: %#v", bulkPayload)
	}
	if replicaCalls != 1 {
		t.Fatalf("expected one replica batch request, got %d", replicaCalls)
	}
	if replicaItems != 2 {
		t.Fatalf("expected replica batch to contain 2 items, got %d", replicaItems)
	}
}

func TestHandleIndex_RemotePrimaryUsesBatchReplication(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 5
	docID := findDocIDForShard(t, shardID, "remote-primary")

	primaryBatchCalls := 0
	primaryIndexCalls := 0
	primaryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/index_batch":
			primaryBatchCalls++

			var req internalIndexBatchRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode primary batch request: %v", err)
			}
			if !req.Replicate {
				t.Fatalf("expected primary batch request to replicate")
			}
			if req.IndexName != "events" || req.Day != day || req.ShardID != shardID {
				t.Fatalf("unexpected primary batch request: %#v", req)
			}
			if len(req.Items) != 1 || req.Items[0].DocID != docID {
				t.Fatalf("unexpected primary batch items: %#v", req.Items)
			}

			writeJSON(w, http.StatusOK, internalIndexBatchResponse{
				OK:       true,
				Index:    req.IndexName,
				Day:      req.Day,
				Shard:    req.ShardID,
				Primary:  "n2",
				Replicas: []string{"n2", "n1"},
				Indexed:  1,
				Acks:     2,
				Quorum:   2,
			})
		case "/internal/index":
			primaryIndexCalls++
			t.Fatalf("unexpected single-document primary path hit")
		default:
			t.Fatalf("unexpected primary path: %s", r.URL.Path)
		}
	}))
	defer primaryServer.Close()

	s.membersMu.Lock()
	s.members["n2"] = NodeInfo{ID: "n2", Addr: primaryServer.URL}
	s.membersMu.Unlock()

	setTestRoute(s, "events", day, shardID, []string{"n2", "n1"})

	body := fmt.Sprintf(`{"id":"%s","timestamp":"%sT09:00:00Z","message":"remote primary event"}`, docID, day)
	resp, err := http.Post(ts.URL+"/index?index=events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("index request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode index response: %v", err)
	}
	if payload["doc_id"] != docID {
		t.Fatalf("expected doc_id %q, got %#v", docID, payload["doc_id"])
	}
	if primaryBatchCalls != 1 {
		t.Fatalf("expected one batch call to primary, got %d", primaryBatchCalls)
	}
	if primaryIndexCalls != 0 {
		t.Fatalf("expected zero single-doc primary calls, got %d", primaryIndexCalls)
	}
}

func TestHandleSearch_CachesReplicaAfterFailover(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 0

	n2SearchCalls := 0
	n2HealthCalls := 0
	replica1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/search_shard":
			n2SearchCalls++
			http.Error(w, "search shard unavailable", http.StatusServiceUnavailable)
		case "/healthz":
			n2HealthCalls++
			http.Error(w, "unexpected health probe", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected replica1 path: %s", r.URL.Path)
		}
	}))
	defer replica1.Close()

	n3SearchCalls := 0
	n3HealthCalls := 0
	replica2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/search_shard":
			n3SearchCalls++
			writeJSON(w, http.StatusOK, SearchShardResponse{
				Hits: []ShardHit{{
					Index:  "events",
					Day:    day,
					Shard:  shardID,
					Score:  1,
					DocID:  "cached-hit",
					Source: Document{"id": "cached-hit", "message": "cached replica result"},
				}},
			})
		case "/healthz":
			n3HealthCalls++
			http.Error(w, "unexpected health probe", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected replica2 path: %s", r.URL.Path)
		}
	}))
	defer replica2.Close()

	s.membersMu.Lock()
	s.members["n2"] = NodeInfo{ID: "n2", Addr: replica1.URL}
	s.members["n3"] = NodeInfo{ID: "n3", Addr: replica2.URL}
	s.membersMu.Unlock()

	setTestRoute(s, "events", day, shardID, []string{"n2", "n3"})

	searchURL := ts.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&q=" + url.QueryEscape("cached") + "&k=10"
	for i := 0; i < 2; i++ {
		resp, err := http.Get(searchURL)
		if err != nil {
			t.Fatalf("search request %d failed: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("expected search status 200, got %d", resp.StatusCode)
		}

		var payload struct {
			Hits          []ShardHit `json:"hits"`
			PartialErrors []string   `json:"partial_errors"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			t.Fatalf("decode search response %d: %v", i+1, err)
		}
		resp.Body.Close()

		if len(payload.Hits) != 1 || payload.Hits[0].DocID != "cached-hit" {
			t.Fatalf("unexpected search hits on request %d: %#v", i+1, payload.Hits)
		}
		if len(payload.PartialErrors) != 0 {
			t.Fatalf("expected no partial errors on request %d, got %#v", i+1, payload.PartialErrors)
		}
	}

	if n2SearchCalls != 1 {
		t.Fatalf("expected failing replica to be tried once, got %d", n2SearchCalls)
	}
	if n3SearchCalls != 2 {
		t.Fatalf("expected healthy replica to serve both searches, got %d", n3SearchCalls)
	}
	if n2HealthCalls != 0 || n3HealthCalls != 0 {
		t.Fatalf("expected zero health probes, got n2=%d n3=%d", n2HealthCalls, n3HealthCalls)
	}
}

func TestHandleSearchShard_DoesNotCreateMissingShard(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 3
	if s.localShardExists("events", day, shardID) {
		t.Fatalf("expected shard to be absent before read")
	}

	body := `{"index_name":"events","day":"2026-03-21","shard_id":3,"query":"missing","k":10}`
	resp, err := http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("search shard request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503 for missing shard, got %d", resp.StatusCode)
	}
	if s.localShardExists("events", day, shardID) {
		t.Fatalf("expected missing shard read to stay non-mutating")
	}
}

func TestHandleIndex_PartialReplicaFailureSkipsStaleReplicaUntilRepair(t *testing.T) {
	primary, primaryTS := newNamedTestHTTPServer(t, "n1")

	var healthySearchCalls atomic.Int32
	healthy, healthyTS := newWrappedTestHTTPServer(t, "n2", func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/internal/search_shard" {
				healthySearchCalls.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	})

	var staleSearchCalls atomic.Int32
	var staleWritesEnabled atomic.Bool
	stale, staleTS := newWrappedTestHTTPServer(t, "n3", func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/internal/index_batch":
				if !staleWritesEnabled.Load() {
					http.Error(w, "replica write unavailable", http.StatusServiceUnavailable)
					return
				}
			case "/internal/search_shard":
				staleSearchCalls.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	})

	day := "2026-03-21"
	shardID := 7
	docID := findDocIDForShard(t, shardID, "repair-stale-replica")
	doc := Document{
		"id":        docID,
		"timestamp": day + "T09:00:00Z",
		"message":   "repair-token",
	}

	for _, srv := range []*Server{primary, healthy, stale} {
		setTestRoute(srv, "events", day, shardID, []string{"n1", "n2", "n3"})
	}

	primary.membersMu.Lock()
	primary.members = map[string]NodeInfo{
		"n1": {ID: "n1", Addr: primaryTS.URL},
		"n2": {ID: "n2", Addr: healthyTS.URL},
		"n3": {ID: "n3", Addr: staleTS.URL},
	}
	primary.membersMu.Unlock()

	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal index document: %v", err)
	}

	resp, err := http.Post(primaryTS.URL+"/index?index=events", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("index request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected partial replica failure to keep quorum, got %d", resp.StatusCode)
	}

	var indexPayload struct {
		OK     bool     `json:"ok"`
		Errors []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&indexPayload); err != nil {
		t.Fatalf("decode index response: %v", err)
	}
	if !indexPayload.OK {
		t.Fatalf("expected quorum write to succeed, got %#v", indexPayload)
	}
	if len(indexPayload.Errors) != 1 || !strings.Contains(indexPayload.Errors[0], "n3") {
		t.Fatalf("expected n3 replication error, got %#v", indexPayload.Errors)
	}
	if !primary.replicaNeedsRepair("events", day, shardID, "n3") {
		t.Fatalf("expected n3 to be marked out-of-sync after failed replication")
	}

	primary.membersMu.Lock()
	primary.members = map[string]NodeInfo{
		"n2": {ID: "n2", Addr: healthyTS.URL},
		"n3": {ID: "n3", Addr: staleTS.URL},
	}
	primary.membersMu.Unlock()

	searchResp, err := http.Get(primaryTS.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&q=repair-token&k=10")
	if err != nil {
		t.Fatalf("search request before repair failed: %v", err)
	}
	defer searchResp.Body.Close()

	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected search status 200 before repair, got %d", searchResp.StatusCode)
	}

	var searchPayload struct {
		Hits []ShardHit `json:"hits"`
	}
	if err := json.NewDecoder(searchResp.Body).Decode(&searchPayload); err != nil {
		t.Fatalf("decode search before repair: %v", err)
	}
	if len(searchPayload.Hits) != 1 || searchPayload.Hits[0].DocID != docID {
		t.Fatalf("unexpected search hits before repair: %#v", searchPayload.Hits)
	}
	if healthySearchCalls.Load() == 0 {
		t.Fatalf("expected healthy replica to serve the search while stale replica is blocked")
	}
	if staleSearchCalls.Load() != 0 {
		t.Fatalf("expected stale replica to be skipped before repair, got %d search calls", staleSearchCalls.Load())
	}

	staleWritesEnabled.Store(true)
	waitForTestCondition(t, 5*time.Second, 50*time.Millisecond, "stale replica to repair", func() (bool, error) {
		idx, err := stale.openShardIndex("events", day, shardID)
		if err != nil {
			return false, err
		}
		count, err := idx.DocCount()
		if err != nil {
			return false, err
		}
		return count == 1, nil
	})

	waitForTestCondition(t, 5*time.Second, 50*time.Millisecond, "primary to clear replica repair marker", func() (bool, error) {
		return !primary.replicaNeedsRepair("events", day, shardID, "n3"), nil
	})

	primary.membersMu.Lock()
	primary.members = map[string]NodeInfo{
		"n3": {ID: "n3", Addr: staleTS.URL},
	}
	primary.membersMu.Unlock()

	searchResp, err = http.Get(primaryTS.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&q=repair-token&k=10")
	if err != nil {
		t.Fatalf("search request after repair failed: %v", err)
	}
	defer searchResp.Body.Close()

	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected search status 200 after repair, got %d", searchResp.StatusCode)
	}

	searchPayload = struct {
		Hits []ShardHit `json:"hits"`
	}{}
	if err := json.NewDecoder(searchResp.Body).Decode(&searchPayload); err != nil {
		t.Fatalf("decode search after repair: %v", err)
	}
	if len(searchPayload.Hits) != 1 || searchPayload.Hits[0].DocID != docID {
		t.Fatalf("unexpected search hits after repair: %#v", searchPayload.Hits)
	}
	if staleSearchCalls.Load() == 0 {
		t.Fatalf("expected repaired replica to serve searches once caught up")
	}
}

func TestSyncShardAssignment_StreamsDocsFromReplica(t *testing.T) {
	target, targetTS := newNamedTestHTTPServer(t, "n1")

	streamCalls := 0
	source, sourceTS := newWrappedTestHTTPServer(t, "n2", func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/internal/stream_docs":
				streamCalls++
			case "/internal/dump_docs":
				t.Fatalf("unexpected legacy dump_docs path hit during shard sync")
			}
			next.ServeHTTP(w, r)
		})
	})

	day := "2026-03-21"
	shardID := 9
	setTestRoute(target, "events", day, shardID, []string{"n1", "n2"})
	setTestRoute(source, "events", day, shardID, []string{"n2"})

	target.membersMu.Lock()
	target.members = map[string]NodeInfo{
		"n1": {ID: "n1", Addr: targetTS.URL},
		"n2": {ID: "n2", Addr: sourceTS.URL},
	}
	target.membersMu.Unlock()

	indexTestDocument(t, source, "events", day, shardID, "sync-a", Document{
		"id":        "sync-a",
		"timestamp": day + "T10:00:00Z",
		"message":   "first streamed doc",
	})
	indexTestDocument(t, source, "events", day, shardID, "sync-b", Document{
		"id":        "sync-b",
		"timestamp": day + "T10:05:00Z",
		"message":   "second streamed doc",
	})

	task := shardSyncTask{
		current: RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n1", "n2"},
			Version:   2,
		},
		previous: RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n2"},
			Version:   1,
		},
	}

	if err := target.syncShardAssignment(context.Background(), task); err != nil {
		t.Fatalf("sync shard assignment failed: %v", err)
	}
	if streamCalls == 0 {
		t.Fatalf("expected shard sync to use streamed docs endpoint")
	}

	idx, err := target.openExistingShardIndex("events", day, shardID)
	if err != nil {
		t.Fatalf("open restored shard: %v", err)
	}
	count, err := idx.DocCount()
	if err != nil {
		t.Fatalf("count restored docs: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 restored docs, got %d", count)
	}
}

func TestHandleSearch_GenericNumericAndDateFields(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	setTestRoute(s, "events", day, 0, []string{"n1"})

	indexTestDocument(t, s, "events", day, 0, "generic-a", Document{
		"id":          "generic-a",
		"timestamp":   day + "T09:00:00Z",
		"latency_ms":  123,
		"observed_at": "2026-03-21T09:00:00Z",
		"service":     "api",
	})
	indexTestDocument(t, s, "events", day, 0, "generic-b", Document{
		"id":          "generic-b",
		"timestamp":   day + "T09:05:00Z",
		"latency_ms":  12,
		"observed_at": "2026-03-20T23:00:00Z",
		"service":     "worker",
	})

	numericResp, err := http.Get(ts.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&q=" + url.QueryEscape("latency_ms:>=100") + "&k=10")
	if err != nil {
		t.Fatalf("numeric search request failed: %v", err)
	}
	defer numericResp.Body.Close()

	if numericResp.StatusCode != http.StatusOK {
		t.Fatalf("expected numeric search status 200, got %d", numericResp.StatusCode)
	}

	var numericPayload struct {
		Hits []ShardHit `json:"hits"`
	}
	if err := json.NewDecoder(numericResp.Body).Decode(&numericPayload); err != nil {
		t.Fatalf("decode numeric search response: %v", err)
	}
	if len(numericPayload.Hits) != 1 || numericPayload.Hits[0].DocID != "generic-a" {
		t.Fatalf("unexpected numeric search hits: %#v", numericPayload.Hits)
	}

	dateResp, err := http.Get(ts.URL + "/search?index=events&day=" + url.QueryEscape(day) + "&q=" + url.QueryEscape(`observed_at:>="2026-03-21T00:00:00Z"`) + "&k=10")
	if err != nil {
		t.Fatalf("date search request failed: %v", err)
	}
	defer dateResp.Body.Close()

	if dateResp.StatusCode != http.StatusOK {
		t.Fatalf("expected date search status 200, got %d", dateResp.StatusCode)
	}

	var datePayload struct {
		Hits []ShardHit `json:"hits"`
	}
	if err := json.NewDecoder(dateResp.Body).Decode(&datePayload); err != nil {
		t.Fatalf("decode date search response: %v", err)
	}
	if len(datePayload.Hits) != 1 || datePayload.Hits[0].DocID != "generic-a" {
		t.Fatalf("unexpected date search hits: %#v", datePayload.Hits)
	}
}

func TestBuildRoutingRebalanceUpdates_AddsNewNodeToExistingDay(t *testing.T) {
	day := "2026-03-21"
	indexName := "events"

	initialMembers := []NodeInfo{
		{ID: "n1", Addr: "http://n1:8081"},
		{ID: "n2", Addr: "http://n2:8081"},
		{ID: "n3", Addr: "http://n3:8081"},
		{ID: "n4", Addr: "http://n4:8081"},
	}
	expandedMembers := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: "http://n1:8081"},
		"n2": {ID: "n2", Addr: "http://n2:8081"},
		"n3": {ID: "n3", Addr: "http://n3:8081"},
		"n4": {ID: "n4", Addr: "http://n4:8081"},
		"n5": {ID: "n5", Addr: "http://n5:8081"},
	}

	routes := make(map[string]RoutingEntry, enforcedShardsPerDay)
	for shardID, replicas := range generateRouting(initialMembers, enforcedShardsPerDay, 3) {
		entry := RoutingEntry{
			IndexName: indexName,
			Day:       day,
			ShardID:   shardID,
			Replicas:  replicas,
			Version:   1,
			UpdatedAt: "2026-03-21T00:00:00Z",
		}
		routes[routingMapKey(indexName, day, shardID)] = entry
	}

	updates := buildRoutingRebalanceUpdates(expandedMembers, routes)
	if len(updates) == 0 {
		t.Fatalf("expected rebalance to produce route updates")
	}

	updatedRoutes := make(map[string]RoutingEntry, len(routes))
	for key, route := range routes {
		updatedRoutes[key] = route
	}
	for _, update := range updates {
		updatedRoutes[routingMapKey(update.IndexName, update.Day, update.ShardID)] = update
	}

	n5ShardCount := 0
	for _, route := range updatedRoutes {
		if route.IndexName != indexName || route.Day != day {
			continue
		}
		if len(route.Replicas) != 3 {
			t.Fatalf("expected replication factor 3, got %d for shard %d", len(route.Replicas), route.ShardID)
		}
		if routeHasReplica(route, "n5") {
			n5ShardCount++
		}
	}

	if n5ShardCount == 0 {
		t.Fatalf("expected rebalanced routing to assign shards to n5")
	}
}

func TestBuildRoutingRebalanceUpdates_ExcludesDrainedNodeWhenCapacityAllows(t *testing.T) {
	day := "2026-03-21"
	indexName := "events"

	members := []NodeInfo{
		{ID: "n1", Addr: "http://n1:8081"},
		{ID: "n2", Addr: "http://n2:8081"},
		{ID: "n3", Addr: "http://n3:8081"},
		{ID: "n4", Addr: "http://n4:8081"},
	}
	routes := make(map[string]RoutingEntry, enforcedShardsPerDay)
	for shardID, replicas := range generateRouting(members, enforcedShardsPerDay, 3) {
		routes[routingMapKey(indexName, day, shardID)] = RoutingEntry{
			IndexName: indexName,
			Day:       day,
			ShardID:   shardID,
			Replicas:  replicas,
			Version:   1,
			UpdatedAt: "2026-03-21T00:00:00Z",
		}
	}

	updatedMembers := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: "http://n1:8081"},
		"n2": {ID: "n2", Addr: "http://n2:8081"},
		"n3": {ID: "n3", Addr: "http://n3:8081"},
		"n4": {ID: "n4", Addr: "http://n4:8081", DrainRequested: true},
	}

	updates := buildRoutingRebalanceUpdates(updatedMembers, routes)
	if len(updates) == 0 {
		t.Fatalf("expected rebalance to update routes when node becomes drained")
	}

	finalRoutes := make(map[string]RoutingEntry, len(routes))
	for key, route := range routes {
		finalRoutes[key] = route
	}
	for _, route := range updates {
		finalRoutes[routingMapKey(route.IndexName, route.Day, route.ShardID)] = route
	}

	for _, route := range finalRoutes {
		if route.IndexName != indexName || route.Day != day {
			continue
		}
		if routeHasReplica(route, "n4") {
			t.Fatalf("expected drained node n4 to be removed from shard %d replicas=%v", route.ShardID, route.Replicas)
		}
	}
}

func TestRebalanceReplicaOrder_PromotesWarmPrimaryAndKeepsNewReplicasBack(t *testing.T) {
	current := []string{"n2", "n1", "n3"}
	desired := []string{"n5", "n2", "n1"}

	got := rebalanceReplicaOrder(current, desired)
	want := []string{"n2", "n1", "n5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected replica order: got %v want %v", got, want)
	}

	current = []string{"n1", "n2", "n3"}
	desired = []string{"n2", "n5", "n1"}

	got = rebalanceReplicaOrder(current, desired)
	want = []string{"n2", "n1", "n5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected replica order: got %v want %v", got, want)
	}
}

func TestHasRecentOfflineNodes_RespectsGraceDrainAndRecovery(t *testing.T) {
	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)

	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	s.offlineMu.Lock()
	s.offlineStates = map[string]NodeOfflineState{
		"n2": {
			NodeID:       "n2",
			Addr:         "http://n2:8081",
			MissingSince: now.Add(-10 * time.Minute).Format(time.RFC3339),
		},
	}
	s.offlineMu.Unlock()

	if !s.hasRecentOfflineNodes(now) {
		t.Fatalf("expected recent offline node to block rebalance")
	}

	s.drainMu.Lock()
	s.drainStates = map[string]NodeDrainState{
		"n2": {
			NodeID:      "n2",
			RequestedAt: now.Format(time.RFC3339),
		},
	}
	s.drainMu.Unlock()

	if s.hasRecentOfflineNodes(now) {
		t.Fatalf("expected drained offline node to stop blocking rebalance")
	}

	s.drainMu.Lock()
	s.drainStates = map[string]NodeDrainState{}
	s.drainMu.Unlock()
	s.membersMu.Lock()
	s.members = map[string]NodeInfo{
		"n2": {ID: "n2", Addr: "http://n2:8081"},
	}
	s.membersMu.Unlock()

	if s.hasRecentOfflineNodes(now) {
		t.Fatalf("expected recovered node to stop blocking rebalance")
	}
}

func TestOfflineStateExpired_AfterGracePeriod(t *testing.T) {
	now := time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)

	recent := NodeOfflineState{
		NodeID:       "n2",
		MissingSince: now.Add(-14*time.Minute - 59*time.Second).Format(time.RFC3339),
	}
	if offlineStateExpired(recent, now) {
		t.Fatalf("expected node within grace period to stay non-expired")
	}

	expired := NodeOfflineState{
		NodeID:       "n2",
		MissingSince: now.Add(-15 * time.Minute).Format(time.RFC3339),
	}
	if !offlineStateExpired(expired, now) {
		t.Fatalf("expected node at grace threshold to expire")
	}

	invalid := NodeOfflineState{
		NodeID:       "n2",
		MissingSince: "not-a-time",
	}
	if !offlineStateExpired(invalid, now) {
		t.Fatalf("expected invalid offline marker timestamp to expire defensively")
	}
}

func TestMemberOnlineStable_AfterGracePeriod(t *testing.T) {
	now := time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC)

	recent := NodeInfo{
		ID:        "n2",
		StartedAt: now.Add(-14*time.Minute - 59*time.Second).Format(time.RFC3339),
	}
	if memberOnlineStable(recent, now) {
		t.Fatalf("expected node within online grace period to stay non-stable")
	}

	stable := NodeInfo{
		ID:        "n2",
		StartedAt: now.Add(-15 * time.Minute).Format(time.RFC3339),
	}
	if !memberOnlineStable(stable, now) {
		t.Fatalf("expected node at online grace threshold to become stable")
	}

	invalid := NodeInfo{
		ID:        "n2",
		StartedAt: "not-a-time",
	}
	if memberOnlineStable(invalid, now) {
		t.Fatalf("expected invalid started_at to stay non-stable")
	}
}

func TestAutoResumableNodes_OnlyResumesAutoDrainsAfterOnlineGrace(t *testing.T) {
	now := time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC)

	members := map[string]NodeInfo{
		"n2": {
			ID:        "n2",
			Addr:      "http://n2:8081",
			StartedAt: now.Add(-16 * time.Minute).Format(time.RFC3339),
		},
		"n3": {
			ID:        "n3",
			Addr:      "http://n3:8081",
			StartedAt: now.Add(-10 * time.Minute).Format(time.RFC3339),
		},
		"n4": {
			ID:        "n4",
			Addr:      "http://n4:8081",
			StartedAt: now.Add(-20 * time.Minute).Format(time.RFC3339),
		},
	}
	drainStates := map[string]NodeDrainState{
		"n2": {
			NodeID:      "n2",
			RequestedAt: now.Add(-30 * time.Minute).Format(time.RFC3339),
			Auto:        true,
		},
		"n3": {
			NodeID:      "n3",
			RequestedAt: now.Add(-30 * time.Minute).Format(time.RFC3339),
			Auto:        true,
		},
		"n4": {
			NodeID:      "n4",
			RequestedAt: now.Add(-30 * time.Minute).Format(time.RFC3339),
			Auto:        false,
		},
		"n5": {
			NodeID:      "n5",
			RequestedAt: now.Add(-30 * time.Minute).Format(time.RFC3339),
			Auto:        true,
		},
	}

	got := autoResumableNodes(members, drainStates, now)
	want := []string{"n2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected auto-resumable nodes: got %v want %v", got, want)
	}
}

func TestIndexDayExpired_RetainsBoundaryDay(t *testing.T) {
	now := time.Date(2026, 3, 22, 15, 30, 0, 0, time.UTC)

	if indexDayExpired("2026-02-21", 30, now) {
		t.Fatalf("expected cutoff day to be retained")
	}
	if !indexDayExpired("2026-02-20", 30, now) {
		t.Fatalf("expected day before cutoff to expire")
	}
	if indexDayExpired("invalid-day", 30, now) {
		t.Fatalf("expected invalid day to stay non-expired defensively")
	}
}

func TestCleanupExpiredLocalShardDays_RemovesExpiredUnroutedDay(t *testing.T) {
	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	now := time.Date(2026, 3, 22, 12, 0, 0, 0, time.UTC)
	expiredDay := "2026-02-20"
	retainedDay := "2026-02-21"

	s.indexRetentionMu.Lock()
	s.indexRetentionPolicies["test1"] = IndexRetentionPolicy{
		IndexName:     "test1",
		RetentionDays: 30,
		UpdatedAt:     now.Format(time.RFC3339),
	}
	s.indexRetentionMu.Unlock()

	setTestRoute(s, "test1", retainedDay, 0, []string{"n1"})

	indexTestDocument(t, s, "test1", expiredDay, 0, "expired-doc", Document{
		"id":        "expired-doc",
		"timestamp": expiredDay + "T10:00:00Z",
		"message":   "expired shard document",
	})
	indexTestDocument(t, s, "test1", retainedDay, 0, "retained-doc", Document{
		"id":        "retained-doc",
		"timestamp": retainedDay + "T10:00:00Z",
		"message":   "retained shard document",
	})

	if _, err := os.Stat(s.shardDayPath("test1", expiredDay)); err != nil {
		t.Fatalf("expected expired shard day path to exist before cleanup: %v", err)
	}
	if _, err := os.Stat(s.shardDayPath("test1", retainedDay)); err != nil {
		t.Fatalf("expected retained shard day path to exist before cleanup: %v", err)
	}

	if err := s.cleanupExpiredLocalShardDays(now); err != nil {
		t.Fatalf("cleanupExpiredLocalShardDays returned error: %v", err)
	}

	if _, err := os.Stat(s.shardDayPath("test1", expiredDay)); !os.IsNotExist(err) {
		t.Fatalf("expected expired shard day path to be removed, got err=%v", err)
	}
	if _, err := os.Stat(s.shardDayPath("test1", retainedDay)); err != nil {
		t.Fatalf("expected retained shard day path to remain, got err=%v", err)
	}

	s.mu.RLock()
	_, expiredCached := s.indexes[partitionKey("test1", expiredDay, 0)]
	_, retainedCached := s.indexes[partitionKey("test1", retainedDay, 0)]
	s.mu.RUnlock()
	if expiredCached {
		t.Fatalf("expected expired shard cache entry to be removed")
	}
	if !retainedCached {
		t.Fatalf("expected retained shard cache entry to remain")
	}
}

func TestShouldRebalanceForMemberChange_IgnoresRemovalButHandlesAddition(t *testing.T) {
	previous := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: "http://n1:8081"},
		"n2": {ID: "n2", Addr: "http://n2:8081"},
	}
	currentWithoutN2 := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: "http://n1:8081"},
	}
	if shouldRebalanceForMemberChange(previous, currentWithoutN2) {
		t.Fatalf("expected member removal alone to wait for offline drain grace period")
	}

	currentWithNewNode := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: "http://n1:8081"},
		"n2": {ID: "n2", Addr: "http://n2:8081"},
		"n3": {ID: "n3", Addr: "http://n3:8081"},
	}
	if !shouldRebalanceForMemberChange(previous, currentWithNewNode) {
		t.Fatalf("expected new member to trigger rebalance")
	}
}

func TestHandleAvailableIndexes_ReturnsSortedIndexesAndDays(t *testing.T) {
	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	setTestRoute(s, "logs", "2026-03-22", 1, []string{"n1"})
	setTestRoute(s, "events", "2026-03-21", 0, []string{"n1"})
	setTestRoute(s, "events", "2026-03-20", 1, []string{"n1"})

	req := httptest.NewRequest(http.MethodGet, "/admin/indexes", nil)
	rec := httptest.NewRecorder()
	s.handleAvailableIndexes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var payload struct {
		Indexes []struct {
			Name string   `json:"name"`
			Days []string `json:"days"`
		} `json:"indexes"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(payload.Indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %d", len(payload.Indexes))
	}
	if payload.Indexes[0].Name != "events" || payload.Indexes[1].Name != "logs" {
		t.Fatalf("unexpected index ordering: %#v", payload.Indexes)
	}
	if !reflect.DeepEqual(payload.Indexes[0].Days, []string{"2026-03-20", "2026-03-21"}) {
		t.Fatalf("unexpected events days: %#v", payload.Indexes[0].Days)
	}
	if !reflect.DeepEqual(payload.Indexes[1].Days, []string{"2026-03-22"}) {
		t.Fatalf("unexpected logs days: %#v", payload.Indexes[1].Days)
	}
}

func TestHandleAvailableIndexes_IncludesRetentionPolicies(t *testing.T) {
	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	setTestRoute(s, "test1", "2026-03-21", 0, []string{"n1"})
	s.indexRetentionMu.Lock()
	s.indexRetentionPolicies["test1"] = IndexRetentionPolicy{
		IndexName:     "test1",
		RetentionDays: 30,
		UpdatedAt:     "2026-03-22T00:00:00Z",
	}
	s.indexRetentionPolicies["archive"] = IndexRetentionPolicy{
		IndexName:     "archive",
		RetentionDays: 7,
		UpdatedAt:     "2026-03-22T00:00:00Z",
	}
	s.indexRetentionMu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/admin/indexes", nil)
	rec := httptest.NewRecorder()
	s.handleAvailableIndexes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var payload struct {
		Indexes []struct {
			Name          string   `json:"name"`
			Days          []string `json:"days"`
			RetentionDays int      `json:"retention_days"`
		} `json:"indexes"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(payload.Indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %#v", payload.Indexes)
	}
	if payload.Indexes[0].Name != "archive" || payload.Indexes[0].RetentionDays != 7 || len(payload.Indexes[0].Days) != 0 {
		t.Fatalf("unexpected archive entry: %#v", payload.Indexes[0])
	}
	if payload.Indexes[1].Name != "test1" || payload.Indexes[1].RetentionDays != 30 || !reflect.DeepEqual(payload.Indexes[1].Days, []string{"2026-03-21"}) {
		t.Fatalf("unexpected test1 entry: %#v", payload.Indexes[1])
	}
}

func TestHandleRouting_WithStatsIncludesShardEventCounts(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 0
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	indexTestDocument(t, s, "events", day, shardID, "stats-a", Document{
		"id":        "stats-a",
		"timestamp": day + "T09:00:00Z",
		"title":     "Stats A",
		"message":   "first stats event",
	})
	indexTestDocument(t, s, "events", day, shardID, "stats-b", Document{
		"id":        "stats-b",
		"timestamp": day + "T09:10:00Z",
		"title":     "Stats B",
		"message":   "second stats event",
	})

	resp, err := http.Get(ts.URL + "/admin/routing?stats=1")
	if err != nil {
		t.Fatalf("routing request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Routing map[string]RoutingEntryStats `json:"routing"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode routing response: %v", err)
	}

	route, ok := payload.Routing[routingMapKey("events", day, shardID)]
	if !ok {
		t.Fatalf("expected route to be present")
	}
	if route.EventCount != 2 {
		t.Fatalf("expected event_count=2, got %d", route.EventCount)
	}
	if route.SizeBytes == 0 {
		t.Fatalf("expected size_bytes to be reported, got %#v", route)
	}
	if route.CountError != "" {
		t.Fatalf("expected no count error, got %q", route.CountError)
	}
}

func TestNormalizeGenericDocument_PreservesGenericFieldsAndPartitionDay(t *testing.T) {
	doc := Document{
		"id":        "evt-1",
		"timestamp": "2026-03-21T12:34:56Z",
		"title":     123,
		"body":      true,
		"message":   456,
		"tags":      []interface{}{"prod", 9, true},
	}

	docID, day, err := normalizeGenericDocument(doc)
	if err != nil {
		t.Fatalf("normalizeGenericDocument returned error: %v", err)
	}

	if docID != "evt-1" {
		t.Fatalf("expected doc id evt-1, got %q", docID)
	}
	if day != "2026-03-21" {
		t.Fatalf("expected day 2026-03-21, got %q", day)
	}
	if got := doc["partition_day"]; got != "2026-03-21" {
		t.Fatalf("expected partition_day to be set, got %#v", got)
	}
	if got := doc["title"]; got != 123 {
		t.Fatalf("expected title to keep original type, got %#v", got)
	}
	if got := doc["body"]; got != true {
		t.Fatalf("expected body to keep original type, got %#v", got)
	}
	if got := doc["message"]; got != 456 {
		t.Fatalf("expected message to keep original type, got %#v", got)
	}
	if !reflect.DeepEqual(doc["tags"], []interface{}{"prod", 9, true}) {
		t.Fatalf("expected tags to keep original shape, got %#v", doc["tags"])
	}
}

func newTestHTTPServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	return newWrappedTestHTTPServer(t, "n1", nil)
}

func newNamedTestHTTPServer(t *testing.T, nodeID string) (*Server, *httptest.Server) {
	t.Helper()
	return newWrappedTestHTTPServer(t, nodeID, nil)
}

func newWrappedTestHTTPServer(t *testing.T, nodeID string, wrap func(http.Handler) http.Handler) (*Server, *httptest.Server) {
	t.Helper()

	s := New(Config{
		Mode:              "both",
		NodeID:            nodeID,
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})

	mux := http.NewServeMux()
	s.registerRoutes(mux)
	var handler http.Handler = mux
	if wrap != nil {
		handler = wrap(handler)
	}
	ts := httptest.NewServer(handler)

	s.membersMu.Lock()
	s.members = map[string]NodeInfo{
		nodeID: {ID: nodeID, Addr: ts.URL},
	}
	s.membersMu.Unlock()

	t.Cleanup(func() {
		ts.Close()
		s.Close()
	})

	return s, ts
}

func waitForTestCondition(t *testing.T, timeout, interval time.Duration, description string, fn func() (bool, error)) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		ok, err := fn()
		if err == nil && ok {
			return
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("timed out waiting for %s: %v", description, err)
			}
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(interval)
	}
}

func setTestRoute(s *Server, indexName, day string, shardID int, replicas []string) {
	s.routingMu.Lock()
	defer s.routingMu.Unlock()
	s.routing[routingMapKey(indexName, day, shardID)] = RoutingEntry{
		IndexName: indexName,
		Day:       day,
		ShardID:   shardID,
		Replicas:  append([]string(nil), replicas...),
		Version:   time.Now().UnixNano(),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

func indexTestDocument(t *testing.T, s *Server, indexName, day string, shardID int, docID string, doc Document) {
	t.Helper()

	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		t.Fatalf("open shard index: %v", err)
	}
	if err := idx.Index(docID, doc); err != nil {
		t.Fatalf("index document: %v", err)
	}
}

func findDocIDForShard(t *testing.T, shardID int, prefix string) string {
	t.Helper()

	for i := 0; i < 100000; i++ {
		candidate := fmt.Sprintf("%s-%d", prefix, i)
		if keyToShard(candidate, enforcedShardsPerDay) == shardID {
			return candidate
		}
	}

	t.Fatalf("could not find doc id for shard %d", shardID)
	return ""
}
