package testdatagen

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRun_EndToEndWithEmbeddedEtcd(t *testing.T) {
	endpoint := startEmbeddedEtcdForGenerator(t)
	client := newGeneratorEtcdClient(t, endpoint)

	markerVersion := "run-test"
	indexName := DefaultIndexName
	dayCount := 3
	eventsPerDay := 12
	days := buildTestDataDays(time.Now().UTC(), dayCount)
	for _, day := range days[:len(days)-1] {
		markerKey := (&Generator{cfg: Config{IndexName: indexName, MarkerVersion: markerVersion}}).markerKey(day)
		if _, err := client.Put(context.Background(), markerKey, time.Now().UTC().Format(time.RFC3339)); err != nil {
			t.Fatalf("preload marker for %s: %v", day, err)
		}
	}
	targetDay := days[len(days)-1]

	var (
		mu               sync.Mutex
		bootstrapDays    []string
		bulkDocCount     int
		observedBulkDays []string
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
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			day := strings.TrimSpace(r.URL.Query().Get("day"))
			mu.Lock()
			bootstrapDays = append(bootstrapDays, day)
			mu.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{"ok": true})
		case "/bulk":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			scanner := bufio.NewScanner(r.Body)
			scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

			count := 0
			bulkDay := ""
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				var doc Document
				if err := json.Unmarshal([]byte(line), &doc); err != nil {
					t.Fatalf("decode bulk doc: %v", err)
				}
				count++
				if bulkDay == "" {
					if tsValue, ok := doc["timestamp"].(string); ok && len(tsValue) >= len("2006-01-02") {
						bulkDay = tsValue[:10]
					}
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("scan bulk request: %v", err)
			}

			mu.Lock()
			bulkDocCount += count
			if bulkDay != "" {
				observedBulkDays = append(observedBulkDays, bulkDay)
			}
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

	g := New(Config{
		ServerURL:         ts.URL,
		ETCDEndpoints:     []string{endpoint},
		IndexName:         indexName,
		DayCount:          dayCount,
		EventsPerDay:      eventsPerDay,
		ReplicationFactor: 2,
		WaitForMembers:    2,
		BulkBatchSize:     eventsPerDay + 5,
		WaitTimeout:       10 * time.Second,
		PollInterval:      10 * time.Millisecond,
		MarkerVersion:     markerVersion,
	})

	if err := g.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if g.etcd != nil {
		t.Fatalf("expected Run to close etcd client")
	}

	mu.Lock()
	gotBootstrapDays := append([]string(nil), bootstrapDays...)
	gotBulkDocCount := bulkDocCount
	gotBulkDays := append([]string(nil), observedBulkDays...)
	mu.Unlock()

	if len(gotBootstrapDays) != 1 || gotBootstrapDays[0] != targetDay {
		t.Fatalf("unexpected bootstrap days: %#v", gotBootstrapDays)
	}
	if gotBulkDocCount != len(buildTestDataDocuments(targetDay, eventsPerDay)) {
		t.Fatalf("expected %d bulk docs, got %d", len(buildTestDataDocuments(targetDay, eventsPerDay)), gotBulkDocCount)
	}
	if len(gotBulkDays) != 1 || gotBulkDays[0] != targetDay {
		t.Fatalf("unexpected bulk day observations: %#v", gotBulkDays)
	}

	for _, day := range days {
		markerKey := (&Generator{cfg: Config{IndexName: indexName, MarkerVersion: markerVersion}}).markerKey(day)
		resp, err := client.Get(context.Background(), markerKey)
		if err != nil {
			t.Fatalf("get marker for %s: %v", day, err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("expected marker for %s, got %d entries", day, len(resp.Kvs))
		}
	}
}

func startEmbeddedEtcdForGenerator(t *testing.T) string {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = fmt.Sprintf("generator-etcd-%d", time.Now().UnixNano())
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(cfg.Dir, "etcd.log")}

	clientURL := mustAllocateGeneratorURL(t)
	peerURL := mustAllocateGeneratorURL(t)
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

func newGeneratorEtcdClient(t *testing.T, endpoint string) *clientv3.Client {
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

func mustAllocateGeneratorURL(t *testing.T) url.URL {
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
