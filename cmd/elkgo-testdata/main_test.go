package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"elkgo/internal/testdatagen"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestSplitCSV_TrimsAndDropsEmptyParts(t *testing.T) {
	got := splitCSV(" http://a:1, ,http://b:2 ,, http://c:3 ")
	want := []string{"http://a:1", "http://b:2", "http://c:3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected splitCSV output: got %#v want %#v", got, want)
	}
}

func TestEnvOr_UsesEnvironmentWhenPresent(t *testing.T) {
	const key = "ELKGO_TESTDATA_MAIN_ENV"
	if err := os.Setenv(key, "from-env"); err != nil {
		t.Fatalf("set env: %v", err)
	}
	defer os.Unsetenv(key)

	if got := envOr(key, "fallback"); got != "from-env" {
		t.Fatalf("expected env value, got %q", got)
	}

	if err := os.Setenv(key, "   "); err != nil {
		t.Fatalf("set blank env: %v", err)
	}
	if got := envOr(key, "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for blank env, got %q", got)
	}
}

func TestEnvOrInt_UsesEnvironmentWhenValid(t *testing.T) {
	const key = "ELKGO_TESTDATA_MAIN_INT_ENV"
	if err := os.Setenv(key, "42"); err != nil {
		t.Fatalf("set env: %v", err)
	}
	defer os.Unsetenv(key)

	if got := envOrInt(key, 7); got != 42 {
		t.Fatalf("expected env int value, got %d", got)
	}

	if err := os.Setenv(key, "bad"); err != nil {
		t.Fatalf("set invalid env: %v", err)
	}
	if got := envOrInt(key, 7); got != 7 {
		t.Fatalf("expected fallback for invalid env, got %d", got)
	}

	if err := os.Setenv(key, "0"); err != nil {
		t.Fatalf("set zero env: %v", err)
	}
	if got := envOrInt(key, 7); got != 7 {
		t.Fatalf("expected fallback for non-positive env, got %d", got)
	}
}

func TestRun_ValidatesAndSeedsThroughRealHTTPServer(t *testing.T) {
	if err := run([]string{"-etcd-endpoints=   "}); err == nil || !strings.Contains(err.Error(), "at least one etcd endpoint is required") {
		t.Fatalf("expected missing endpoint error, got %v", err)
	}

	endpoint := startEmbeddedEtcdForCmd(t)
	client := newCmdEtcdClient(t, endpoint)
	dayCount := 3
	eventsPerDay := 11
	days := testdatagenDaysForMain(t, dayCount)
	markerVersion := "cmd-run-test"
	indexName := testdatagen.DefaultIndexName
	for _, day := range days[:len(days)-1] {
		key := fmt.Sprintf("/distsearch/admin/test-data-loaded/%s/%s/%s", markerVersion, indexName, day)
		if _, err := client.Put(context.Background(), key, time.Now().UTC().Format(time.RFC3339)); err != nil {
			t.Fatalf("preload marker for %s: %v", day, err)
		}
	}
	targetDay := days[len(days)-1]

	var (
		mu            sync.Mutex
		bootstrapDays []string
		bulkDocCount  int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			writeJSON(t, w, http.StatusOK, map[string]any{
				"ok":      true,
				"node_id": "n1",
				"members": map[string]any{"n1": map[string]any{}, "n2": map[string]any{}},
			})
		case "/admin/bootstrap":
			mu.Lock()
			bootstrapDays = append(bootstrapDays, r.URL.Query().Get("day"))
			mu.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{"ok": true})
		case "/bulk":
			scanner := bufio.NewScanner(r.Body)
			scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
			count := 0
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				var doc map[string]any
				if err := json.Unmarshal([]byte(line), &doc); err != nil {
					t.Fatalf("decode bulk doc: %v", err)
				}
				count++
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("scan bulk request: %v", err)
			}
			mu.Lock()
			bulkDocCount += count
			mu.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{
				"ok":      true,
				"index":   indexName,
				"indexed": count,
				"failed":  0,
				"errors":  []string{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	err := run([]string{
		"-server-url=" + ts.URL,
		"-index=" + indexName,
		"-etcd-endpoints=" + endpoint,
		fmt.Sprintf("-days-back=%d", dayCount),
		fmt.Sprintf("-events-per-day=%d", eventsPerDay),
		"-replication-factor=2",
		"-wait-for-members=2",
		"-marker-version=" + markerVersion,
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	mu.Lock()
	gotBootstrapDays := append([]string(nil), bootstrapDays...)
	gotBulkDocCount := bulkDocCount
	mu.Unlock()
	if len(gotBootstrapDays) != 1 || gotBootstrapDays[0] != targetDay {
		t.Fatalf("unexpected bootstrap days: %#v", gotBootstrapDays)
	}
	if gotBulkDocCount != eventsPerDay {
		t.Fatalf("expected %d generated docs, got %d", eventsPerDay, gotBulkDocCount)
	}
}

func startEmbeddedEtcdForCmd(t *testing.T) string {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = fmt.Sprintf("cmd-etcd-%d", time.Now().UnixNano())
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(cfg.Dir, "etcd.log")}

	clientURL := allocateCmdURL(t)
	peerURL := allocateCmdURL(t)
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		etcd.Close()
		t.Fatal("timed out waiting for embedded etcd readiness")
	}
	t.Cleanup(func() {
		etcd.Close()
	})
	return "http://" + etcd.Clients[0].Addr().String()
}

func newCmdEtcdClient(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new etcd client: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func allocateCmdURL(t *testing.T) url.URL {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer ln.Close()

	u, err := url.Parse("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("parse allocated URL: %v", err)
	}
	return *u
}

func testdatagenDaysForMain(t *testing.T, dayCount int) []string {
	t.Helper()

	base := time.Now().UTC().Truncate(24 * time.Hour)
	days := make([]string, 0, dayCount)
	for offset := dayCount - 1; offset >= 0; offset-- {
		days = append(days, base.AddDate(0, 0, -offset).Format("2006-01-02"))
	}
	return days
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode json response: %v", err)
	}
}
