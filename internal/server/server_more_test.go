package server

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleHealthAndStaticPages(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected health status 200, got %d", resp.StatusCode)
	}

	var health struct {
		OK      bool                `json:"ok"`
		NodeID  string              `json:"node_id"`
		Members map[string]NodeInfo `json:"members"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if !health.OK || health.NodeID != "n1" || len(health.Members) != 1 {
		t.Fatalf("unexpected health payload: %#v", health)
	}

	for _, tc := range []struct {
		path         string
		method       string
		wantStatus   int
		wantContains string
	}{
		{path: "/", method: http.MethodGet, wantStatus: http.StatusOK, wantContains: "<title>elkgo search</title>"},
		{path: "/cluster", method: http.MethodGet, wantStatus: http.StatusOK, wantContains: "Cluster dashboard"},
		{path: "/", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
		{path: "/cluster", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
		{path: "/missing", method: http.MethodGet, wantStatus: http.StatusNotFound},
	} {
		req, err := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", tc.method, tc.path, err)
		}
		body := readAllAndClose(t, resp)
		if resp.StatusCode != tc.wantStatus {
			t.Fatalf("%s %s: expected status %d, got %d", tc.method, tc.path, tc.wantStatus, resp.StatusCode)
		}
		if tc.wantContains != "" && !strings.Contains(body, tc.wantContains) {
			t.Fatalf("%s %s: expected body to contain %q, got %q", tc.method, tc.path, tc.wantContains, body)
		}
		if tc.path == "/" && tc.method == http.MethodGet {
			if !strings.Contains(body, `id="day_from"`) || !strings.Contains(body, `id="day_to"`) {
				t.Fatalf("%s %s: expected range date inputs in search form", tc.method, tc.path)
			}
			if strings.Contains(body, `id="day"`) {
				t.Fatalf("%s %s: expected legacy single-day input to be removed", tc.method, tc.path)
			}
			if !strings.Contains(body, "All indexes") {
				t.Fatalf("%s %s: expected all-index search option", tc.method, tc.path)
			}
			if !strings.Contains(body, `id="k" name="k" class="input-compact"`) {
				t.Fatalf("%s %s: expected compact Top K input", tc.method, tc.path)
			}
		}
		if tc.path == "/cluster" && tc.method == http.MethodGet && !strings.Contains(body, "Index retention") {
			t.Fatalf("%s %s: expected body to contain %q", tc.method, tc.path, "Index retention")
		}
		if tc.path == "/cluster" && tc.method == http.MethodGet && !strings.Contains(body, "<th>Size</th>") {
			t.Fatalf("%s %s: expected body to contain %q", tc.method, tc.path, "<th>Size</th>")
		}
	}

	_ = s
}

func TestHandleInternalIndexDumpAndStreamDocs(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 0
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	doc := `{"index_name":"events","day":"2026-03-21","shard_id":0,"doc":{"id":"evt-1","timestamp":"2026-03-21T12:00:00Z","message":"hello","tags":["a","b"],"meta":{"service":"api"}}}`
	resp, err := http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(doc))
	if err != nil {
		t.Fatalf("internal index request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected internal index status 200, got %d body=%q", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/internal/dump_docs?index=events&day=2026-03-21&shard=0")
	if err != nil {
		t.Fatalf("dump docs request failed: %v", err)
	}
	var dump DumpDocsResponse
	if err := json.NewDecoder(resp.Body).Decode(&dump); err != nil {
		resp.Body.Close()
		t.Fatalf("decode dump docs: %v", err)
	}
	resp.Body.Close()
	if len(dump.Docs) != 1 {
		t.Fatalf("expected 1 dumped doc, got %#v", dump.Docs)
	}
	if dump.Docs[0]["id"] != "evt-1" {
		t.Fatalf("unexpected dumped id: %#v", dump.Docs[0])
	}

	resp, err = http.Get(ts.URL + "/internal/stream_docs?index=events&day=2026-03-21&shard=0")
	if err != nil {
		t.Fatalf("stream docs request failed: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "application/x-ndjson") {
		t.Fatalf("unexpected stream docs content type: %q", got)
	}
	dec := json.NewDecoder(resp.Body)
	var streamed []Document
	for {
		var doc Document
		if err := dec.Decode(&doc); err != nil {
			if strings.Contains(err.Error(), "EOF") {
				break
			}
			t.Fatalf("decode streamed doc: %v", err)
		}
		streamed = append(streamed, doc)
	}
	if len(streamed) != 1 || streamed[0]["id"] != "evt-1" {
		t.Fatalf("unexpected streamed docs: %#v", streamed)
	}

	resp, err = http.Get(ts.URL + "/internal/shard_stats?index=events&day=2026-03-21&shard=0")
	if err != nil {
		t.Fatalf("shard stats request failed: %v", err)
	}
	var stats ShardStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		resp.Body.Close()
		t.Fatalf("decode shard stats: %v", err)
	}
	resp.Body.Close()
	if stats.EventCount != 1 {
		t.Fatalf("expected event count 1, got %#v", stats)
	}
	if stats.SizeBytes == 0 {
		t.Fatalf("expected shard size_bytes to be reported, got %#v", stats)
	}
}

func TestSearchHelpersAndAdmission(t *testing.T) {
	if got := effectiveSearchShardConcurrency(0); got != 1 {
		t.Fatalf("expected zero targets to clamp concurrency to 1, got %d", got)
	}
	if got := effectiveSearchShardConcurrency(2); got != 2 {
		t.Fatalf("expected small target count to cap concurrency, got %d", got)
	}

	if got := compareSearchRefs(searchHitRef{DocID: "same", Score: 1}, searchHitRef{DocID: "same", Score: 1}); got != 0 {
		t.Fatalf("expected identical refs to compare equal, got %d", got)
	}

	heapItems := searchHitMinHeap{
		{DocID: "z", Score: 1},
		{DocID: "a", Score: 1},
	}
	if !heapItems.Less(0, 1) {
		t.Fatalf("expected tie on score to treat lexicographically larger doc id as smaller heap item")
	}

	s := &Server{searchAdmission: make(chan struct{}, 1)}
	if !s.tryAcquireSearchAdmission() {
		t.Fatalf("expected first search admission acquire to succeed")
	}
	if s.tryAcquireSearchAdmission() {
		t.Fatalf("expected second search admission acquire to fail when channel is full")
	}
	s.releaseSearchAdmission()
	if !s.tryAcquireSearchAdmission() {
		t.Fatalf("expected search admission to become available after release")
	}
	s.releaseSearchAdmission()
}

func TestServerHelpersAndTransport(t *testing.T) {
	if got := publicAddrFromListen(":8081"); got != "http://127.0.0.1:8081" {
		t.Fatalf("unexpected public addr from listen: %q", got)
	}
	if got := publicAddrFromListen("https://elkgo.internal:9443"); got != "https://elkgo.internal:9443" {
		t.Fatalf("unexpected https public addr: %q", got)
	}

	s := New(Config{
		Mode:              "coordinator",
		NodeID:            "n1",
		Listen:            ":8081",
		PublicAddr:        "elkgo.internal:9000",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	if !s.isCoordinatorMode() {
		t.Fatalf("expected coordinator mode to be true")
	}
	if got := s.advertisedAddr(); got != "http://elkgo.internal:9000" {
		t.Fatalf("unexpected advertised addr: %q", got)
	}
	if got := s.routingKey("events", "2026-03-21", 7); got != "/distsearch/routing/events/2026-03-21/7" {
		t.Fatalf("unexpected routing key: %q", got)
	}

	s.cacheReplica("events|2026-03-21|7", "n2")
	s.clearReplicaCache()
	if _, ok := s.cachedReplica("events|2026-03-21|7"); ok {
		t.Fatalf("expected replica cache to be cleared")
	}

	for raw, want := range map[string]bool{
		"":      true,
		"1":     true,
		"true":  true,
		"off":   false,
		"false": false,
	} {
		got, err := parseDrainFlag(raw)
		if err != nil {
			t.Fatalf("parseDrainFlag(%q) returned error: %v", raw, err)
		}
		if got != want {
			t.Fatalf("parseDrainFlag(%q): got %v want %v", raw, got, want)
		}
	}
	if _, err := parseDrainFlag("maybe"); err == nil {
		t.Fatalf("expected invalid drain flag error")
	}

	req := httptest.NewRequest(http.MethodGet, "/?stats=true&include_counts=0&enabled=yes", nil)
	if !queryEnabled(req, "stats") || !queryEnabled(req, "enabled") || queryEnabled(req, "include_counts") {
		t.Fatalf("unexpected queryEnabled evaluation")
	}

	errorsOut := make([]string, 0, 1)
	appendBulkError(&errorsOut, "first")
	appendBulkError(&errorsOut, "second")
	if len(errorsOut) != 2 {
		t.Fatalf("expected appendBulkError to add errors, got %#v", errorsOut)
	}
	group := bulkShardGroup{
		indexName: "events",
		day:       "2026-03-21",
		shardID:   7,
		items: []bulkPreparedItem{
			{lineNo: 10},
			{lineNo: 12},
			{lineNo: 11},
		},
	}
	if start, end := group.lineRange(); start != 10 || end != 12 {
		t.Fatalf("unexpected line range: %d-%d", start, end)
	}
	if got := formatBulkGroupError(group, http.ErrHandlerTimeout); !strings.Contains(got, "lines 10-12") {
		t.Fatalf("unexpected bulk group error formatting: %q", got)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ndjson":
			if r.Header.Get("Content-Type") != "application/x-ndjson" {
				t.Fatalf("unexpected NDJSON content type: %q", r.Header.Get("Content-Type"))
			}
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		case "/json":
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		case "/stream_bad":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte("not-json\n"))
		default:
			http.Error(w, "nope", http.StatusBadRequest)
		}
	}))
	defer ts.Close()

	var postResp map[string]any
	if err := s.postNDJSON(httptest.NewRequest(http.MethodGet, "/", nil).Context(), ts.URL+"/ndjson", []Document{{"id": "evt-1"}}, &postResp); err != nil {
		t.Fatalf("postNDJSON returned error: %v", err)
	}
	if postResp["ok"] != true {
		t.Fatalf("unexpected NDJSON response: %#v", postResp)
	}
	if err := s.postNDJSONWithTimeout(httptest.NewRequest(http.MethodGet, "/", nil).Context(), ts.URL+"/bad", []Document{{"id": "evt-2"}}, http.DefaultClient.Timeout, nil); err == nil {
		t.Fatalf("expected postNDJSONWithTimeout error for bad status")
	}

	if err := s.getJSONWithTimeout(httptest.NewRequest(http.MethodGet, "/", nil).Context(), ts.URL+"/bad", http.DefaultClient.Timeout, &postResp); err == nil {
		t.Fatalf("expected getJSONWithTimeout error for bad status")
	}
	if err := s.streamDocuments(httptest.NewRequest(http.MethodGet, "/", nil).Context(), ts.URL+"/stream_bad", func(Document) error { return nil }); err == nil {
		t.Fatalf("expected streamDocuments error for invalid NDJSON")
	}
}

func TestLoggingMiddleware_DelegatesAndLogs(t *testing.T) {
	var logs bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	}()

	handler := loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/hello", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted || rec.Body.String() != "ok" {
		t.Fatalf("unexpected middleware response: code=%d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(logs.String(), "GET /hello") {
		t.Fatalf("expected request log, got %q", logs.String())
	}
}

func readAllAndClose(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return buf.String()
}
