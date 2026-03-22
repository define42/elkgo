package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHandleBulkIngest_PartialFailuresReturnMultiStatus(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/bulk?index=events", nil)
	if err != nil {
		t.Fatalf("new GET bulk request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /bulk failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET /bulk status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/bulk", "application/x-ndjson", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST /bulk without index failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected POST /bulk without index status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	day := "2026-03-21"
	goodShardID := 12
	missingShardID := 13
	goodDocID := findDocIDForShard(t, goodShardID, "bulk-partial-good")
	missingRouteDocID := findDocIDForShard(t, missingShardID, "bulk-partial-missing")
	setTestRoute(s, "events", day, goodShardID, []string{"n1"})

	body := strings.Join([]string{
		`{"id":`,
		fmt.Sprintf(`{"id":"%s","timestamp":"%sT09:00:00Z","message":"good bulk event"}`, goodDocID, day),
		fmt.Sprintf(`{"id":"%s","timestamp":"%sT09:05:00Z","message":"missing route event"}`, missingRouteDocID, day),
	}, "\n") + "\n"

	resp, err = http.Post(ts.URL+"/bulk?index=events", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatalf("partial bulk request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("expected partial bulk status 207, got %d", resp.StatusCode)
	}

	var payload struct {
		OK      bool     `json:"ok"`
		Lines   int      `json:"lines"`
		Indexed int      `json:"indexed"`
		Failed  int      `json:"failed"`
		Errors  []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode partial bulk response: %v", err)
	}
	if payload.OK || payload.Lines != 3 || payload.Indexed != 1 || payload.Failed != 2 {
		t.Fatalf("unexpected partial bulk payload: %#v", payload)
	}
	if len(payload.Errors) != 2 {
		t.Fatalf("expected 2 bulk errors, got %#v", payload.Errors)
	}
	if !strings.Contains(payload.Errors[0], "line 1") && !strings.Contains(payload.Errors[1], "line 1") {
		t.Fatalf("expected malformed JSON error, got %#v", payload.Errors)
	}
	if !strings.Contains(strings.Join(payload.Errors, " "), "line 3") {
		t.Fatalf("expected missing route error, got %#v", payload.Errors)
	}

	searchResp, err := http.Get(ts.URL + "/search?index=events&day_from=" + day + "&day_to=" + day + "&k=10")
	if err != nil {
		t.Fatalf("search after partial bulk failed: %v", err)
	}
	defer searchResp.Body.Close()
	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected search status 200, got %d", searchResp.StatusCode)
	}
	var searchPayload struct {
		Hits []ShardHit `json:"hits"`
	}
	if err := json.NewDecoder(searchResp.Body).Decode(&searchPayload); err != nil {
		t.Fatalf("decode search payload: %v", err)
	}
	if len(searchPayload.Hits) != 1 || searchPayload.Hits[0].DocID != goodDocID {
		t.Fatalf("unexpected search hits after bulk ingest: %#v", searchPayload.Hits)
	}
}

func TestHandleInternalIndexBatch_CoversValidationAndLocalIndexing(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/internal/index_batch", nil)
	if err != nil {
		t.Fatalf("new GET /internal/index_batch request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /internal/index_batch failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET /internal/index_batch status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("invalid JSON /internal/index_batch failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected invalid JSON status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-21","shard_id":1,"items":[]}`))
	if err != nil {
		t.Fatalf("empty batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected empty batch status 200, got %d", resp.StatusCode)
	}
	var emptyResp internalIndexBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&emptyResp); err != nil {
		resp.Body.Close()
		t.Fatalf("decode empty batch response: %v", err)
	}
	resp.Body.Close()
	if !emptyResp.OK || emptyResp.Indexed != 0 {
		t.Fatalf("unexpected empty batch response: %#v", emptyResp)
	}

	bodyNoRoute := `{"index_name":"events","day":"2026-03-21","shard_id":2,"items":[{"doc_id":"evt-no-route","doc":{"id":"evt-no-route","timestamp":"2026-03-21T10:00:00Z"}}]}`
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyNoRoute))
	if err != nil {
		t.Fatalf("no routing batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected no routing status 503, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	day := "2026-03-21"
	remotePrimaryShard := 14
	setTestRoute(s, "events", day, remotePrimaryShard, []string{"n2", "n1"})
	bodyRemotePrimary := fmt.Sprintf(`{"index_name":"events","day":"%s","shard_id":%d,"replicate":true,"items":[{"doc_id":"evt-remote-primary","doc":{"id":"evt-remote-primary","timestamp":"%sT10:05:00Z"}}]}`, day, remotePrimaryShard, day)
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyRemotePrimary))
	if err != nil {
		t.Fatalf("remote primary batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected remote primary status 403, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	nonReplicaShard := 15
	setTestRoute(s, "events", day, nonReplicaShard, []string{"n2"})
	bodyNonReplica := fmt.Sprintf(`{"index_name":"events","day":"%s","shard_id":%d,"items":[{"doc_id":"evt-non-replica","doc":{"id":"evt-non-replica","timestamp":"%sT10:10:00Z"}}]}`, day, nonReplicaShard, day)
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyNonReplica))
	if err != nil {
		t.Fatalf("non-replica batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected non-replica status 403, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	localPrimaryShard := 16
	setTestRoute(s, "events", day, localPrimaryShard, []string{"n1"})
	bodyPrimary := fmt.Sprintf(`{"index_name":"events","day":"%s","shard_id":%d,"replicate":true,"items":[{"doc_id":"evt-primary","doc":{"id":"evt-primary","timestamp":"%sT10:15:00Z","message":"primary batch"}}]}`, day, localPrimaryShard, day)
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyPrimary))
	if err != nil {
		t.Fatalf("local primary batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected local primary batch status 200, got %d body=%q", resp.StatusCode, body)
	}
	var primaryResp internalIndexBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&primaryResp); err != nil {
		resp.Body.Close()
		t.Fatalf("decode local primary batch response: %v", err)
	}
	resp.Body.Close()
	if !primaryResp.OK || primaryResp.Acks != 1 || primaryResp.Quorum != 1 || primaryResp.Indexed != 1 {
		t.Fatalf("unexpected local primary batch response: %#v", primaryResp)
	}

	localReplicaShard := 17
	setTestRoute(s, "events", day, localReplicaShard, []string{"n1"})
	bodyLocalReplica := fmt.Sprintf(`{"index_name":"events","day":"%s","shard_id":%d,"items":[{"doc":{"id":"evt-local","timestamp":"%sT10:20:00Z","message":"local replica batch"}}]}`, day, localReplicaShard, day)
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyLocalReplica))
	if err != nil {
		t.Fatalf("local replica batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected local replica batch status 200, got %d body=%q", resp.StatusCode, body)
	}
	var localReplicaResp internalIndexBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&localReplicaResp); err != nil {
		resp.Body.Close()
		t.Fatalf("decode local replica batch response: %v", err)
	}
	resp.Body.Close()
	if !localReplicaResp.OK || localReplicaResp.Indexed != 1 {
		t.Fatalf("unexpected local replica batch response: %#v", localReplicaResp)
	}

	docs, err := s.dumpAllDocs("events", day, localReplicaShard)
	if err != nil {
		t.Fatalf("dump local replica docs: %v", err)
	}
	if len(docs) != 1 || docs[0]["id"] != "evt-local" {
		t.Fatalf("unexpected local replica docs: %#v", docs)
	}

	mismatchShard := 18
	setTestRoute(s, "events", day, mismatchShard, []string{"n1"})
	bodyMismatch := fmt.Sprintf(`{"index_name":"events","day":"%s","shard_id":%d,"items":[{"doc":{"id":"evt-mismatch","timestamp":"2026-03-22T10:25:00Z","message":"wrong day"}}]}`, day, mismatchShard)
	resp, err = http.Post(ts.URL+"/internal/index_batch", "application/json", strings.NewReader(bodyMismatch))
	if err != nil {
		t.Fatalf("mismatch day batch request failed: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected mismatch day status 500, got %d body=%q", resp.StatusCode, body)
	}
	resp.Body.Close()
}

func TestHandleRoutingAndFetchShardStats_CoversShardQueriesAndFailover(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	replicaMissing, replicaMissingTS := newNamedTestHTTPServer(t, "n2")
	replicaHealthy, replicaHealthyTS := newNamedTestHTTPServer(t, "n3")

	day := "2026-03-21"
	shardID := 19
	setTestRoute(replicaMissing, "events", day, shardID, []string{"n2"})
	setTestRoute(replicaHealthy, "events", day, shardID, []string{"n3"})
	indexTestDocument(t, replicaHealthy, "events", day, shardID, "stats-doc", Document{
		"id":        "stats-doc",
		"timestamp": day + "T11:00:00Z",
		"message":   "healthy shard",
	})

	s.membersMu.Lock()
	s.members["n2"] = NodeInfo{ID: "n2", Addr: replicaMissingTS.URL}
	s.members["n3"] = NodeInfo{ID: "n3", Addr: replicaHealthyTS.URL}
	s.membersMu.Unlock()
	setTestRoute(s, "events", day, shardID, []string{"n2", "n3"})

	route, ok := s.getRouting("events", day, shardID)
	if !ok {
		t.Fatalf("expected route to exist for stats test")
	}

	stats, err := s.fetchShardStats(context.Background(), route)
	if err != nil {
		t.Fatalf("fetchShardStats returned error: %v", err)
	}
	if stats.EventCount != 1 {
		t.Fatalf("expected shard event count 1, got %#v", stats)
	}
	if stats.SizeBytes == 0 {
		t.Fatalf("expected shard size_bytes to be reported, got %#v", stats)
	}

	statsEntry := s.routingEntryStats(context.Background(), route)
	if statsEntry.EventCount != 1 || statsEntry.SizeBytes == 0 || statsEntry.CountError != "" {
		t.Fatalf("unexpected routingEntryStats payload: %#v", statsEntry)
	}

	failingOnly := route
	failingOnly.Replicas = []string{"n2"}
	failingEntry := s.routingEntryStats(context.Background(), failingOnly)
	if failingEntry.CountError == "" {
		t.Fatalf("expected failing routing entry stats to include CountError")
	}

	resp, err := http.Get(ts.URL + "/admin/routing?index=events&day=" + day + "&shard=bad")
	if err != nil {
		t.Fatalf("bad shard routing request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad shard status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/admin/routing?index=events&day=" + day + "&shard=999")
	if err != nil {
		t.Fatalf("unknown shard routing request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected unknown shard status 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/admin/routing?index=events&day=" + day + fmt.Sprintf("&shard=%d&stats=1", shardID))
	if err != nil {
		t.Fatalf("routing stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected routing stats status 200, got %d", resp.StatusCode)
	}
	var routingStats RoutingEntryStats
	if err := json.NewDecoder(resp.Body).Decode(&routingStats); err != nil {
		resp.Body.Close()
		t.Fatalf("decode routing stats response: %v", err)
	}
	resp.Body.Close()
	if routingStats.EventCount != 1 || routingStats.SizeBytes == 0 || routingStats.ShardID != shardID {
		t.Fatalf("unexpected routing stats response: %#v", routingStats)
	}

	resp, err = http.Get(ts.URL + "/admin/routing?stats=1")
	if err != nil {
		t.Fatalf("routing map stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected routing map stats status 200, got %d", resp.StatusCode)
	}
	var routingStatsMapResp struct {
		Routing      map[string]RoutingEntryStats `json:"routing"`
		Members      map[string]NodeInfo          `json:"members"`
		ShardsPerDay int                          `json:"shards_per_day"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&routingStatsMapResp); err != nil {
		resp.Body.Close()
		t.Fatalf("decode routing map stats response: %v", err)
	}
	resp.Body.Close()
	routeKey := routingMapKey("events", day, shardID)
	if routingStatsMapResp.ShardsPerDay != enforcedShardsPerDay {
		t.Fatalf("unexpected shards_per_day: %#v", routingStatsMapResp)
	}
	if routingStatsMapResp.Routing[routeKey].EventCount != 1 {
		t.Fatalf("unexpected routing map stats payload: %#v", routingStatsMapResp.Routing)
	}
	if routingStatsMapResp.Routing[routeKey].SizeBytes == 0 {
		t.Fatalf("expected routing map size_bytes to be reported, got %#v", routingStatsMapResp.Routing[routeKey])
	}
	if len(routingStatsMapResp.Members) < 3 {
		t.Fatalf("expected members in routing stats response, got %#v", routingStatsMapResp.Members)
	}

	resp, err = http.Get(ts.URL + "/admin/routing")
	if err != nil {
		t.Fatalf("routing request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected routing status 200, got %d", resp.StatusCode)
	}
	var routingResp struct {
		Routing map[string]RoutingEntry `json:"routing"`
		Members map[string]NodeInfo     `json:"members"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&routingResp); err != nil {
		resp.Body.Close()
		t.Fatalf("decode routing response: %v", err)
	}
	resp.Body.Close()
	if routingResp.Routing[routeKey].ShardID != shardID {
		t.Fatalf("unexpected routing payload: %#v", routingResp.Routing)
	}
	if len(routingResp.Members) < 3 {
		t.Fatalf("expected members in routing response, got %#v", routingResp.Members)
	}
}

func TestShardSyncAndReplicaRepairHelpers(t *testing.T) {
	source, sourceTS := newNamedTestHTTPServer(t, "n2")
	target, _ := newNamedTestHTTPServer(t, "n1")

	target.membersMu.Lock()
	target.members["n2"] = NodeInfo{ID: "n2", Addr: sourceTS.URL}
	target.membersMu.Unlock()

	daySync := "2026-03-21"
	shardSync := 20
	setTestRoute(source, "events", daySync, shardSync, []string{"n2"})
	indexTestDocument(t, source, "events", daySync, shardSync, "sync-doc", Document{
		"id":        "sync-doc",
		"timestamp": daySync + "T12:00:00Z",
		"message":   "synced document",
	})

	oldRoute := RoutingEntry{
		IndexName: "events",
		Day:       daySync,
		ShardID:   shardSync,
		Replicas:  []string{"n2"},
		Version:   2,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	newRoute := RoutingEntry{
		IndexName: "events",
		Day:       daySync,
		ShardID:   shardSync,
		Replicas:  []string{"n2", "n1"},
		Version:   3,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	routeKey := routingMapKey("events", daySync, shardSync)

	tasks := shardSyncTasksForNode("n1", map[string]RoutingEntry{routeKey: oldRoute}, map[string]RoutingEntry{routeKey: newRoute})
	if len(tasks) != 1 {
		t.Fatalf("expected one shard sync task, got %#v", tasks)
	}
	if !target.claimShardSync(newRoute) {
		t.Fatalf("expected first shard sync claim to succeed")
	}
	if target.claimShardSync(newRoute) {
		t.Fatalf("expected duplicate shard sync claim to fail")
	}
	target.finishShardSync(newRoute, false)
	if !target.claimShardSync(newRoute) {
		t.Fatalf("expected shard sync claim to succeed after failed finish")
	}
	target.finishShardSync(newRoute, true)
	if target.claimShardSync(newRoute) {
		t.Fatalf("expected shard sync claim to fail after successful finish at same version")
	}

	asyncRoute := newRoute
	asyncRoute.Version = 4
	target.routingMu.Lock()
	target.routing[routeKey] = asyncRoute
	target.routingMu.Unlock()
	asyncTasks := shardSyncTasksForNode("n1", map[string]RoutingEntry{routeKey: oldRoute}, map[string]RoutingEntry{routeKey: asyncRoute})
	target.notePendingShardSyncTasks(asyncTasks, map[string]RoutingEntry{routeKey: asyncRoute})
	if target.shardReadyForReplicaTraffic("events", daySync, shardSync) {
		t.Fatalf("expected newly assigned shard to stay unready until sync completes")
	}

	target.syncAssignedShardsAsync(asyncTasks)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "async shard sync", func() (bool, error) {
		target.shardSyncMu.Lock()
		syncedVersion := target.shardSyncedVersion[routeKey]
		target.shardSyncMu.Unlock()
		if syncedVersion != asyncRoute.Version {
			return false, nil
		}
		docs, err := target.dumpAllDocs("events", daySync, shardSync)
		if err != nil {
			return false, err
		}
		return len(docs) == 1 && docs[0]["id"] == "sync-doc", nil
	})

	target.shardSyncMu.Lock()
	syncedVersion := target.shardSyncedVersion[routeKey]
	target.shardSyncMu.Unlock()
	if syncedVersion != asyncRoute.Version {
		t.Fatalf("expected synced version %d, got %d", asyncRoute.Version, syncedVersion)
	}

	dayRepair := "2026-03-22"
	shardRepair := 21
	setTestRoute(target, "events", dayRepair, shardRepair, []string{"n1", "n2"})
	setTestRoute(source, "events", dayRepair, shardRepair, []string{"n1", "n2"})
	indexTestDocument(t, target, "events", dayRepair, shardRepair, "repair-doc", Document{
		"id":        "repair-doc",
		"timestamp": dayRepair + "T12:30:00Z",
		"message":   "repair document",
	})

	validRepairKey := replicaRepairMapKey("events", dayRepair, shardRepair, "n2")
	staleRepairKey := replicaRepairMapKey("events", "2026-03-23", 22, "n9")
	target.replicaRepairMu.Lock()
	target.replicaRepairStates[validRepairKey] = ReplicaRepairState{
		IndexName: "events",
		Day:       dayRepair,
		ShardID:   shardRepair,
		NodeID:    "n2",
		MarkedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	target.replicaRepairStates[staleRepairKey] = ReplicaRepairState{
		IndexName: "events",
		Day:       "2026-03-23",
		ShardID:   22,
		NodeID:    "n9",
		MarkedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	target.replicaRepairMu.Unlock()

	if err := target.cleanupObsoleteReplicaRepairStates(); err != nil {
		t.Fatalf("cleanupObsoleteReplicaRepairStates returned error: %v", err)
	}
	if target.replicaNeedsRepair("events", "2026-03-23", 22, "n9") {
		t.Fatalf("expected stale repair state to be removed")
	}
	if !target.replicaNeedsRepair("events", dayRepair, shardRepair, "n2") {
		t.Fatalf("expected live repair state to remain before resume")
	}

	target.resumeReplicaRepairLoops()

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "replica repair", func() (bool, error) {
		if target.replicaNeedsRepair("events", dayRepair, shardRepair, "n2") {
			return false, nil
		}
		docs, err := source.dumpAllDocs("events", dayRepair, shardRepair)
		if err != nil {
			return false, err
		}
		return len(docs) == 1 && docs[0]["id"] == "repair-doc", nil
	})
}

func TestSyncAssignedShardsAsync_RetriesUntilSourceReady(t *testing.T) {
	source, sourceTS := newNamedTestHTTPServer(t, "n2")
	target, _ := newNamedTestHTTPServer(t, "n1")

	day := "2026-03-23"
	shardID := 22
	setTestRoute(source, "events", day, shardID, []string{"n2"})
	indexTestDocument(t, source, "events", day, shardID, "sync-retry-doc", Document{
		"id":        "sync-retry-doc",
		"timestamp": day + "T08:00:00Z",
		"message":   "retry until source is ready",
	})

	var gateMu sync.Mutex
	blocked := true
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateMu.Lock()
		currentlyBlocked := blocked
		gateMu.Unlock()

		if currentlyBlocked && (strings.HasPrefix(r.URL.Path, "/internal/snapshot_shard") || strings.HasPrefix(r.URL.Path, "/internal/stream_docs")) {
			http.Error(w, "source warming up", http.StatusServiceUnavailable)
			return
		}

		req, err := http.NewRequestWithContext(r.Context(), r.Method, sourceTS.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		req.Header = r.Header.Clone()

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	target.membersMu.Lock()
	target.members["n2"] = NodeInfo{ID: "n2", Addr: proxy.URL}
	target.membersMu.Unlock()

	oldRoute := RoutingEntry{
		IndexName: "events",
		Day:       day,
		ShardID:   shardID,
		Replicas:  []string{"n2"},
		Version:   10,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	newRoute := RoutingEntry{
		IndexName: "events",
		Day:       day,
		ShardID:   shardID,
		Replicas:  []string{"n2", "n1"},
		Version:   11,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	routeKey := routingMapKey("events", day, shardID)
	target.routingMu.Lock()
	target.routing[routeKey] = newRoute
	target.routingMu.Unlock()

	tasks := shardSyncTasksForNode("n1", map[string]RoutingEntry{routeKey: oldRoute}, map[string]RoutingEntry{routeKey: newRoute})
	target.notePendingShardSyncTasks(tasks, map[string]RoutingEntry{routeKey: newRoute})
	target.syncAssignedShardsAsync(tasks)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "pending sync before source ready", func() (bool, error) {
		return !target.shardReadyForReplicaTraffic("events", day, shardID), nil
	})

	gateMu.Lock()
	blocked = false
	gateMu.Unlock()

	waitForTestCondition(t, 10*time.Second, 50*time.Millisecond, "async shard sync retry", func() (bool, error) {
		docs, err := target.dumpAllDocs("events", day, shardID)
		if err != nil {
			return false, err
		}
		if len(docs) != 1 || docs[0]["id"] != "sync-retry-doc" {
			return false, nil
		}
		return target.shardReadyForReplicaTraffic("events", day, shardID), nil
	})
}

func TestStreamShardToReplica_RepairBypassesSyncGate(t *testing.T) {
	source, _ := newNamedTestHTTPServer(t, "n1")
	target, targetTS := newNamedTestHTTPServer(t, "n2")

	day := "2026-03-24"
	shardID := 23
	setTestRoute(source, "events", day, shardID, []string{"n1"})
	setTestRoute(target, "events", day, shardID, []string{"n2"})
	indexTestDocument(t, source, "events", day, shardID, "repair-stream-doc", Document{
		"id":        "repair-stream-doc",
		"timestamp": day + "T10:15:00Z",
		"message":   "repair stream can write while syncing",
	})

	source.membersMu.Lock()
	source.members["n2"] = NodeInfo{ID: "n2", Addr: targetTS.URL}
	source.membersMu.Unlock()

	route, ok := target.getRouting("events", day, shardID)
	if !ok {
		t.Fatalf("expected target route to exist")
	}
	target.notePendingShardSyncTasks([]shardSyncTask{{
		current: route,
	}}, map[string]RoutingEntry{
		routingMapKey("events", day, shardID): route,
	})
	if target.shardReadyForReplicaTraffic("events", day, shardID) {
		t.Fatalf("expected target shard to remain unready while syncing")
	}

	restored, err := source.streamShardToReplica(route, "n2")
	if err != nil {
		t.Fatalf("stream shard to replica while syncing: %v", err)
	}
	if restored != 1 {
		t.Fatalf("expected 1 restored document, got %d", restored)
	}

	docs, err := target.dumpAllDocs("events", day, shardID)
	if err != nil {
		t.Fatalf("dump docs after repair stream: %v", err)
	}
	if len(docs) != 1 || docs[0]["id"] != "repair-stream-doc" {
		t.Fatalf("unexpected streamed repair docs: %#v", docs)
	}
}

func TestRouteHelpersAndReplicaRepairLoopEdges(t *testing.T) {
	s, _ := newNamedTestHTTPServer(t, "n1")

	fallback := RoutingEntry{
		IndexName: "events",
		Day:       "2026-03-25",
		ShardID:   5,
		Replicas:  []string{"n1", "n2"},
		Version:   12,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	route, err := s.currentRouteForShard("events", "2026-03-25", 5, fallback)
	if err != nil {
		t.Fatalf("currentRouteForShard with fallback: %v", err)
	}
	if route.Version != fallback.Version {
		t.Fatalf("expected fallback route version %d, got %#v", fallback.Version, route)
	}

	setTestRoute(s, "events", "2026-03-25", 5, []string{"n1"})
	route, err = s.currentRouteForShard("events", "2026-03-25", 5, RoutingEntry{})
	if err != nil {
		t.Fatalf("currentRouteForShard with stored route: %v", err)
	}
	if len(route.Replicas) != 1 || route.Replicas[0] != "n1" {
		t.Fatalf("unexpected stored route: %#v", route)
	}

	if _, err := s.currentRouteForShard("missing", "2026-03-25", 5, RoutingEntry{}); err == nil {
		t.Fatalf("expected missing route error")
	}

	for _, tc := range []struct {
		err  error
		want bool
	}{
		{err: fmt.Errorf("primary not assigned to this node"), want: true},
		{err: fmt.Errorf("primary not registered"), want: true},
		{err: fmt.Errorf("routing not initialized for shard"), want: true},
		{err: fmt.Errorf("replica syncing"), want: false},
		{err: nil, want: false},
	} {
		if got := shouldRetryRouteError(tc.err); got != tc.want {
			t.Fatalf("shouldRetryRouteError(%v)=%v want %v", tc.err, got, tc.want)
		}
	}

	missingKey := replicaRepairMapKey("events", "2026-03-26", 7, "n2")
	s.replicaRepairMu.Lock()
	s.replicaRepairStates[missingKey] = ReplicaRepairState{IndexName: "events", Day: "2026-03-26", ShardID: 7, NodeID: "n2"}
	s.replicaRepairMu.Unlock()
	s.noteReplicaRepairRequest(missingKey)
	if !s.claimReplicaRepairWorker(missingKey) {
		t.Fatalf("expected missing-route repair worker claim to succeed")
	}
	s.repairReplicaLoop("events", "2026-03-26", 7, "n2")
	if s.replicaNeedsRepair("events", "2026-03-26", 7, "n2") {
		t.Fatalf("expected missing-route repair state to be cleared")
	}
	if s.latestReplicaRepairRequest(missingKey) != 0 {
		t.Fatalf("expected missing-route repair request to be cleared")
	}

	day := "2026-03-27"
	shardID := 8
	setTestRoute(s, "events", day, shardID, []string{"n2", "n1"})
	nonPrimaryKey := replicaRepairMapKey("events", day, shardID, "n2")
	s.replicaRepairMu.Lock()
	s.replicaRepairStates[nonPrimaryKey] = ReplicaRepairState{IndexName: "events", Day: day, ShardID: shardID, NodeID: "n2"}
	s.replicaRepairMu.Unlock()
	s.noteReplicaRepairRequest(nonPrimaryKey)
	if !s.claimReplicaRepairWorker(nonPrimaryKey) {
		t.Fatalf("expected non-primary repair worker claim to succeed")
	}
	s.repairReplicaLoop("events", day, shardID, "n2")
	if !s.replicaNeedsRepair("events", day, shardID, "n2") {
		t.Fatalf("expected non-primary repair state to remain for another primary")
	}
}

func TestHandleIndexRetention_WithoutEtcd(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	s.indexRetentionMu.Lock()
	s.indexRetentionPolicies["events"] = IndexRetentionPolicy{
		IndexName:     "events",
		RetentionDays: 30,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	s.indexRetentionMu.Unlock()

	resp, err := http.Get(ts.URL + "/admin/index_retention")
	if err != nil {
		t.Fatalf("GET /admin/index_retention: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected list status 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/admin/index_retention?index=events")
	if err != nil {
		t.Fatalf("GET /admin/index_retention?index=events: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected single policy status 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/admin/index_retention?index=missing")
	if err != nil {
		t.Fatalf("GET /admin/index_retention?index=missing: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected missing policy status 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/admin/index_retention", nil)
	if err != nil {
		t.Fatalf("new POST request without index: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /admin/index_retention without index: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected POST without index status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodPost, ts.URL+"/admin/index_retention?index=events&retention_days=bad", nil)
	if err != nil {
		t.Fatalf("new POST request with invalid retention: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /admin/index_retention invalid retention: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected POST invalid retention status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodPost, ts.URL+"/admin/index_retention?index=events&retention_days=14", nil)
	if err != nil {
		t.Fatalf("new POST request with etcd missing: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /admin/index_retention with missing etcd: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected POST missing etcd status 500, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/admin/index_retention", nil)
	if err != nil {
		t.Fatalf("new DELETE request without index: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE /admin/index_retention without index: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected DELETE without index status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/admin/index_retention?index=events", nil)
	if err != nil {
		t.Fatalf("new DELETE request with etcd missing: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE /admin/index_retention with missing etcd: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected DELETE missing etcd status 500, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestInternalShardReadHandlers_ValidationAndAvailability(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	for _, tc := range []struct {
		path       string
		method     string
		wantStatus int
	}{
		{path: "/internal/dump_docs?index=events&day=2026-03-21&shard=bad", method: http.MethodGet, wantStatus: http.StatusBadRequest},
		{path: "/internal/stream_docs?index=events&day=2026-03-21&shard=bad", method: http.MethodGet, wantStatus: http.StatusBadRequest},
		{path: "/internal/shard_stats?index=events&day=2026-03-21&shard=bad", method: http.MethodGet, wantStatus: http.StatusBadRequest},
		{path: "/internal/dump_docs?index=events&day=2026-03-21&shard=1", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
		{path: "/internal/stream_docs?index=events&day=2026-03-21&shard=1", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
		{path: "/internal/shard_stats?index=events&day=2026-03-21&shard=1", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
		{path: "/internal/dump_docs?index=events&day=2026-03-21&shard=1", method: http.MethodGet, wantStatus: http.StatusForbidden},
		{path: "/internal/stream_docs?index=events&day=2026-03-21&shard=1", method: http.MethodGet, wantStatus: http.StatusForbidden},
		{path: "/internal/shard_stats?index=events&day=2026-03-21&shard=1", method: http.MethodGet, wantStatus: http.StatusForbidden},
	} {
		req, err := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		if err != nil {
			t.Fatalf("new request for %s %s: %v", tc.method, tc.path, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", tc.method, tc.path, err)
		}
		if resp.StatusCode != tc.wantStatus {
			body := readAllAndClose(t, resp)
			t.Fatalf("%s %s: expected status %d, got %d body=%q", tc.method, tc.path, tc.wantStatus, resp.StatusCode, body)
		}
		resp.Body.Close()
	}

	day := "2026-03-21"
	shardID := 22
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	for _, path := range []string{
		fmt.Sprintf("/internal/dump_docs?index=events&day=%s&shard=%d", day, shardID),
		fmt.Sprintf("/internal/stream_docs?index=events&day=%s&shard=%d", day, shardID),
		fmt.Sprintf("/internal/shard_stats?index=events&day=%s&shard=%d", day, shardID),
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			body := readAllAndClose(t, resp)
			t.Fatalf("GET %s: expected status 503, got %d body=%q", path, resp.StatusCode, body)
		}
		resp.Body.Close()
	}
}
