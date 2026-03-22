package testdatagen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRun_ValidatesRequiredConfiguration(t *testing.T) {
	g := New(Config{ETCDEndpoints: []string{"http://127.0.0.1:2379"}})
	g.cfg.ServerURL = ""
	if err := g.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "server URL is required") {
		t.Fatalf("expected missing server URL error, got %v", err)
	}

	g = New(Config{ServerURL: "http://127.0.0.1:8081"})
	g.cfg.ETCDEndpoints = nil
	if err := g.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "at least one etcd endpoint is required") {
		t.Fatalf("expected missing etcd endpoints error, got %v", err)
	}
}

func TestWaitForCluster_SucceedsAfterRetry(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		members := map[string]any{"n1": map[string]any{}}
		if attempts >= 2 {
			members["n2"] = map[string]any{}
		}
		writeJSON(t, w, http.StatusOK, map[string]any{
			"ok":      attempts >= 2,
			"node_id": "n1",
			"members": members,
		})
	}))
	defer ts.Close()

	g := New(Config{
		ServerURL:      ts.URL,
		WaitForMembers: 2,
		WaitTimeout:    2 * time.Second,
		PollInterval:   10 * time.Millisecond,
	})

	if err := g.waitForCluster(context.Background()); err != nil {
		t.Fatalf("waitForCluster returned error: %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected at least 2 wait attempts, got %d", attempts)
	}
}

func TestWaitForCluster_TimeoutAndCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"ok":      false,
			"node_id": "n1",
			"members": map[string]any{"n1": map[string]any{}},
		})
	}))
	defer ts.Close()

	g := New(Config{
		ServerURL:      ts.URL,
		WaitForMembers: 2,
		WaitTimeout:    50 * time.Millisecond,
		PollInterval:   10 * time.Millisecond,
	})
	if err := g.waitForCluster(context.Background()); err == nil || !strings.Contains(err.Error(), "timed out waiting for 2 members") {
		t.Fatalf("expected timeout error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.waitForCluster(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestBootstrapDay_PostsAndHandlesFailure(t *testing.T) {
	requests := make([]string, 0, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		if len(requests) == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "bootstrap failed", http.StatusBadGateway)
	}))
	defer ts.Close()

	g := New(Config{
		ServerURL:         ts.URL,
		IndexName:         "logs",
		ReplicationFactor: 2,
	})
	if err := g.bootstrapDay(context.Background(), "2026-03-21"); err != nil {
		t.Fatalf("bootstrapDay returned error: %v", err)
	}
	if len(requests) != 1 || !strings.HasPrefix(requests[0], http.MethodPost+" /admin/bootstrap?") {
		t.Fatalf("unexpected bootstrap request log: %#v", requests)
	}

	err := g.bootstrapDay(context.Background(), "2026-03-22")
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected bootstrap status error, got %v", err)
	}
}

func TestGeneratorHTTPHelpersAndMarkerKey(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			writeJSON(t, w, http.StatusOK, map[string]any{"ok": true})
		case "/ndjson":
			if r.Header.Get("Content-Type") != "application/x-ndjson" {
				t.Fatalf("unexpected content type: %q", r.Header.Get("Content-Type"))
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"indexed": 1})
		default:
			http.Error(w, "nope", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	g := New(Config{
		ServerURL:      ts.URL,
		IndexName:      "logs",
		MarkerVersion:  "v9",
		HTTPTimeout:    time.Second,
		BulkTimeout:    time.Second,
		WaitTimeout:    time.Second,
		PollInterval:   time.Millisecond,
		WaitForMembers: 1,
	})

	var payload struct {
		OK bool `json:"ok"`
	}
	if err := g.getJSON(context.Background(), ts.URL+"/ok", &payload); err != nil {
		t.Fatalf("getJSON returned error: %v", err)
	}
	if !payload.OK {
		t.Fatalf("expected ok payload, got %#v", payload)
	}

	if err := g.getJSON(context.Background(), ts.URL+"/fail", &payload); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected getJSON status error, got %v", err)
	}

	var ndjsonResp struct {
		Indexed int `json:"indexed"`
	}
	if err := g.postNDJSONWithTimeout(context.Background(), ts.URL+"/ndjson", []Document{{"id": "evt-1"}}, time.Second, &ndjsonResp); err != nil {
		t.Fatalf("postNDJSONWithTimeout returned error: %v", err)
	}
	if ndjsonResp.Indexed != 1 {
		t.Fatalf("expected indexed=1, got %#v", ndjsonResp)
	}

	if err := g.postNDJSONWithTimeout(context.Background(), ts.URL+"/fail", []Document{{"id": "evt-2"}}, time.Second, nil); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected NDJSON status error, got %v", err)
	}

	if got := g.markerKey("2026-03-21"); got != "/distsearch/admin/test-data-loaded/v9/logs/2026-03-21" {
		t.Fatalf("unexpected marker key: %q", got)
	}
}

func TestSleepWithContextAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled sleep, got %v", err)
	}

	if err := sleepWithContext(context.Background(), 5*time.Millisecond); err != nil {
		t.Fatalf("expected successful sleep, got %v", err)
	}

	g := New(Config{})
	if err := g.Close(); err != nil {
		t.Fatalf("expected Close on nil etcd to succeed, got %v", err)
	}
}

func TestGetJSON_DecodesResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{"name": "elkgo", "count": 2}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer ts.Close()

	g := New(Config{ServerURL: ts.URL})
	var payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	if err := g.getJSON(context.Background(), ts.URL, &payload); err != nil {
		t.Fatalf("getJSON returned error: %v", err)
	}
	if payload.Name != "elkgo" || payload.Count != 2 {
		t.Fatalf("unexpected decoded payload: %#v", payload)
	}
}
