package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
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

	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})

	mux := http.NewServeMux()
	s.registerRoutes(mux)
	ts := httptest.NewServer(mux)

	s.membersMu.Lock()
	s.members = map[string]NodeInfo{
		"n1": {ID: "n1", Addr: ts.URL},
	}
	s.membersMu.Unlock()

	t.Cleanup(func() {
		ts.Close()
		s.Close()
	})

	return s, ts
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
