package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type embeddedEtcd struct {
	server   *embed.Etcd
	endpoint string
}

func TestEmbeddedEtcdLifecycle_WatchesAndRepairState(t *testing.T) {
	cluster := startEmbeddedEtcd(t)
	client := newEmbeddedEtcdClient(t, cluster.endpoint)

	s := newEtcdBackedServer(t, cluster.endpoint, "n1", "both", "http://127.0.0.1:18081")
	mustRegisterAndLoadServerState(t, s)

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.watchMembers(watchCtx)
	go s.watchDrainStates(watchCtx)
	go s.watchOfflineStates(watchCtx)
	go s.watchRouting(watchCtx)
	go s.watchReplicaRepairStates(watchCtx)

	member := MemberLease{
		NodeID:    "n2",
		Addr:      "http://127.0.0.1:18082/",
		StartedAt: time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339),
	}
	putEtcdJSON(t, client, s.memberPrefix+member.NodeID, member)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "member watch load", func() (bool, error) {
		members := s.snapshotMembers()
		got, ok := members["n2"]
		return ok && got.Addr == "http://127.0.0.1:18082" && !got.DrainRequested, nil
	})

	drainState := NodeDrainState{
		NodeID:      "n2",
		RequestedAt: time.Now().UTC().Format(time.RFC3339),
		Auto:        false,
	}
	putEtcdJSON(t, client, s.drainPrefix+"n2", drainState)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "drain watch load", func() (bool, error) {
		member, ok := s.snapshotMembers()["n2"]
		return ok && member.DrainRequested, nil
	})

	day := "2026-03-21"
	route := RoutingEntry{
		IndexName: "events",
		Day:       day,
		ShardID:   7,
		Replicas:  []string{"n1", "n2"},
		Version:   1,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	putEtcdJSON(t, client, s.routingKey(route.IndexName, route.Day, route.ShardID), route)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "routing watch load", func() (bool, error) {
		got, ok := s.getRouting(route.IndexName, route.Day, route.ShardID)
		return ok && len(got.Replicas) == 2 && got.Replicas[1] == "n2", nil
	})

	repairState := ReplicaRepairState{
		IndexName: route.IndexName,
		Day:       route.Day,
		ShardID:   route.ShardID,
		NodeID:    "n2",
		MarkedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	putEtcdJSON(t, client, s.replicaRepairKey(route.IndexName, route.Day, route.ShardID, "n2"), repairState)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "replica repair watch load", func() (bool, error) {
		return s.replicaNeedsRepair(route.IndexName, route.Day, route.ShardID, "n2"), nil
	})

	putEtcdJSON(t, client, s.offlinePrefix+"n9", NodeOfflineState{
		NodeID:       "n9",
		Addr:         "http://127.0.0.1:19009",
		MissingSince: time.Now().UTC().Format(time.RFC3339),
	})
	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "offline watch load", func() (bool, error) {
		_, ok := s.snapshotOfflineStates()["n9"]
		return ok, nil
	})

	deleteEtcdKey(t, client, s.memberPrefix+"n2")
	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "reconcile offline marker on member removal", func() (bool, error) {
		resp, err := client.Get(context.Background(), s.offlinePrefix+"n2")
		if err != nil {
			return false, err
		}
		return len(resp.Kvs) == 1, nil
	})

	putEtcdJSON(t, client, s.memberPrefix+member.NodeID, member)
	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "offline marker cleared on member return", func() (bool, error) {
		resp, err := client.Get(context.Background(), s.offlinePrefix+"n2")
		if err != nil {
			return false, err
		}
		return len(resp.Kvs) == 0, nil
	})

	route.Replicas = []string{"n1"}
	route.Version = 2
	route.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	putEtcdJSON(t, client, s.routingKey(route.IndexName, route.Day, route.ShardID), route)

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "obsolete replica repair state cleanup", func() (bool, error) {
		return !s.replicaNeedsRepair(route.IndexName, route.Day, route.ShardID, "n2"), nil
	})

	missingReplicaRoute := RoutingEntry{
		IndexName: "events",
		Day:       "2026-03-22",
		ShardID:   8,
		Replicas:  []string{"n1", "n7"},
		Version:   1,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	s.routingMu.Lock()
	s.routing[routingMapKey(missingReplicaRoute.IndexName, missingReplicaRoute.Day, missingReplicaRoute.ShardID)] = missingReplicaRoute
	s.routingMu.Unlock()

	if err := s.ensureOfflineMarkersForMissingRouteReplicas(context.Background()); err != nil {
		t.Fatalf("ensureOfflineMarkersForMissingRouteReplicas returned error: %v", err)
	}
	resp, err := client.Get(context.Background(), s.offlinePrefix+"n7")
	if err != nil {
		t.Fatalf("get ensured offline marker: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected offline marker for missing route replica, got %d entries", len(resp.Kvs))
	}

	canceledCtx, canceled := context.WithCancel(context.Background())
	canceled()
	s.offlineDrainLoop(canceledCtx)
}

func TestAdminHandlersAndRebalance_WithEmbeddedEtcd(t *testing.T) {
	cluster := startEmbeddedEtcd(t)

	coordinator, coordinatorTS := newEtcdBackedHTTPServer(t, cluster.endpoint, "n1", "both")
	_, _ = newEtcdBackedHTTPServer(t, cluster.endpoint, "n2", "both")
	_, _ = newEtcdBackedHTTPServer(t, cluster.endpoint, "n3", "both")

	if err := coordinator.loadMembers(context.Background()); err != nil {
		t.Fatalf("loadMembers before bootstrap: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, coordinatorTS.URL+"/admin/bootstrap", nil)
	if err != nil {
		t.Fatalf("build GET bootstrap request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /admin/bootstrap failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET /admin/bootstrap status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	for _, tc := range []string{
		"/admin/bootstrap?day=2026-03-21",
		"/admin/bootstrap?index=events",
		"/admin/bootstrap?index=events&day=bad-day",
	} {
		req, err := http.NewRequest(http.MethodPost, coordinatorTS.URL+tc, nil)
		if err != nil {
			t.Fatalf("build bootstrap request %s: %v", tc, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", tc, err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			body := readAllAndClose(t, resp)
			t.Fatalf("POST %s: expected status 400, got %d body=%q", tc, resp.StatusCode, body)
		}
		resp.Body.Close()
	}

	day := "2026-03-23"
	req, err = http.NewRequest(http.MethodPost, coordinatorTS.URL+"/admin/bootstrap?index=events&day="+day+"&replication_factor=2", nil)
	if err != nil {
		t.Fatalf("build successful bootstrap request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("successful bootstrap request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected successful bootstrap status 200, got %d body=%q", resp.StatusCode, body)
	}
	var bootstrapPayload struct {
		OK                bool           `json:"ok"`
		ReplicationFactor int            `json:"replication_factor"`
		Routes            []RoutingEntry `json:"routes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bootstrapPayload); err != nil {
		resp.Body.Close()
		t.Fatalf("decode bootstrap response: %v", err)
	}
	resp.Body.Close()
	if !bootstrapPayload.OK || bootstrapPayload.ReplicationFactor != 2 || len(bootstrapPayload.Routes) != enforcedShardsPerDay {
		t.Fatalf("unexpected bootstrap payload: %#v", bootstrapPayload)
	}

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "routing present after bootstrap", func() (bool, error) {
		return len(coordinator.snapshotRouting()) == enforcedShardsPerDay, nil
	})

	req, err = http.NewRequest(http.MethodGet, coordinatorTS.URL+"/admin/nodes/drain?node_id=n2", nil)
	if err != nil {
		t.Fatalf("build GET drain request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET drain request failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET drain status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	for _, tc := range []string{
		"/admin/nodes/drain",
		"/admin/nodes/drain?node_id=n2&drain=maybe",
		"/admin/nodes/drain?node_id=missing",
	} {
		req, err := http.NewRequest(http.MethodPost, coordinatorTS.URL+tc, nil)
		if err != nil {
			t.Fatalf("build drain request %s: %v", tc, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", tc, err)
		}
		want := http.StatusBadRequest
		if strings.Contains(tc, "missing") {
			want = http.StatusNotFound
		}
		if resp.StatusCode != want {
			body := readAllAndClose(t, resp)
			t.Fatalf("POST %s: expected status %d, got %d body=%q", tc, want, resp.StatusCode, body)
		}
		resp.Body.Close()
	}

	req, err = http.NewRequest(http.MethodPost, coordinatorTS.URL+"/admin/nodes/drain?node_id=n2&drain=true", nil)
	if err != nil {
		t.Fatalf("build drain enable request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("drain enable request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected drain enable status 200, got %d body=%q", resp.StatusCode, body)
	}
	resp.Body.Close()

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "drained node excluded from routing", func() (bool, error) {
		for _, route := range coordinator.snapshotRouting() {
			if routeHasReplica(route, "n2") {
				return false, nil
			}
		}
		return true, nil
	})

	req, err = http.NewRequest(http.MethodPost, coordinatorTS.URL+"/admin/nodes/drain?node_id=n2&drain=false", nil)
	if err != nil {
		t.Fatalf("build drain disable request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("drain disable request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body := readAllAndClose(t, resp)
		t.Fatalf("expected drain disable status 200, got %d body=%q", resp.StatusCode, body)
	}
	resp.Body.Close()

	waitForTestCondition(t, 5*time.Second, 25*time.Millisecond, "rebalanced node returns to routing", func() (bool, error) {
		for _, route := range coordinator.snapshotRouting() {
			if routeHasReplica(route, "n2") {
				return true, nil
			}
		}
		return false, nil
	})
}

func TestAutoDrainResumeAndRun_WithEmbeddedEtcd(t *testing.T) {
	cluster := startEmbeddedEtcd(t)
	client := newEmbeddedEtcdClient(t, cluster.endpoint)

	s := newEtcdBackedServer(t, cluster.endpoint, "n1", "both", "http://127.0.0.1:18091")
	mustRegisterAndLoadServerState(t, s)

	putEtcdJSON(t, client, s.offlinePrefix+"n9", NodeOfflineState{
		NodeID:       "n9",
		Addr:         "http://127.0.0.1:19009",
		MissingSince: time.Now().UTC().Add(-offlineDrainGracePeriod - time.Minute).Format(time.RFC3339),
	})
	if err := s.loadOfflineStates(context.Background()); err != nil {
		t.Fatalf("loadOfflineStates before auto-drain: %v", err)
	}

	changed, err := s.maybeAutoDrainExpiredOfflineNodes(context.Background())
	if err != nil {
		t.Fatalf("maybeAutoDrainExpiredOfflineNodes returned error: %v", err)
	}
	if !changed {
		t.Fatalf("expected auto-drain to report changes")
	}
	resp, err := client.Get(context.Background(), s.drainPrefix+"n9")
	if err != nil {
		t.Fatalf("get auto-drain key: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected auto-drain key for n9, got %d entries", len(resp.Kvs))
	}

	putEtcdJSON(t, client, s.memberPrefix+"n9", MemberLease{
		NodeID:    "n9",
		Addr:      "http://127.0.0.1:19009",
		StartedAt: time.Now().UTC().Add(-onlineResumeGracePeriod - time.Minute).Format(time.RFC3339),
	})
	if err := s.loadMembers(context.Background()); err != nil {
		t.Fatalf("loadMembers before auto-resume: %v", err)
	}
	changed, err = s.maybeAutoResumeRecoveredNodes(context.Background())
	if err != nil {
		t.Fatalf("maybeAutoResumeRecoveredNodes returned error: %v", err)
	}
	if !changed {
		t.Fatalf("expected auto-resume to report changes")
	}
	resp, err = client.Get(context.Background(), s.drainPrefix+"n9")
	if err != nil {
		t.Fatalf("get drain key after auto-resume: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("expected auto-resume to clear drain key, got %d entries", len(resp.Kvs))
	}

	if err := s.loadMembers(context.Background()); err != nil {
		t.Fatalf("loadMembers after auto-resume: %v", err)
	}
	if err := s.loadOfflineStates(context.Background()); err != nil {
		t.Fatalf("loadOfflineStates after auto-resume: %v", err)
	}
	if s.hasRecentOfflineNodes(time.Now().UTC()) {
		t.Fatalf("expected no recent offline nodes after auto-drain + resume path")
	}

	runServer := New(Config{
		Mode:              "both",
		NodeID:            "run-node",
		Listen:            "bad-listen-address",
		PublicAddr:        "http://127.0.0.1:18100",
		DataDir:           t.TempDir(),
		ETCDEndpoints:     []string{cluster.endpoint},
		ReplicationFactor: 1,
	})
	defer runServer.Close()

	err = runServer.Run()
	if err == nil {
		t.Fatalf("expected Run to fail on invalid listen address")
	}
	if !strings.Contains(err.Error(), "listen") && !strings.Contains(err.Error(), "missing port") {
		t.Fatalf("unexpected Run error: %v", err)
	}

	resp, err = client.Get(context.Background(), runServer.memberPrefix+"run-node")
	if err != nil {
		t.Fatalf("get registered member from Run: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected Run to register member before listen failure, got %d entries", len(resp.Kvs))
	}
}

func startEmbeddedEtcd(t *testing.T) embeddedEtcd {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = fmt.Sprintf("etcd-%d", time.Now().UnixNano())
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(cfg.Dir, "etcd.log")}

	clientURL := mustAllocateURL(t)
	peerURL := mustAllocateURL(t)
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

	return embeddedEtcd{
		server:   etcd,
		endpoint: "http://" + etcd.Clients[0].Addr().String(),
	}
}

func newEmbeddedEtcdClient(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new embedded etcd client: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func newEtcdBackedServer(t *testing.T, endpoint, nodeID, mode, publicAddr string) *Server {
	t.Helper()

	s := New(Config{
		Mode:              mode,
		NodeID:            nodeID,
		Listen:            ":0",
		PublicAddr:        publicAddr,
		DataDir:           t.TempDir(),
		ETCDEndpoints:     []string{endpoint},
		ReplicationFactor: 2,
	})
	t.Cleanup(func() {
		s.Close()
	})
	if err := s.connectEtcd(context.Background()); err != nil {
		t.Fatalf("connectEtcd: %v", err)
	}
	return s
}

func newEtcdBackedHTTPServer(t *testing.T, endpoint, nodeID, mode string) (*Server, *httptest.Server) {
	t.Helper()

	s := New(Config{
		Mode:              mode,
		NodeID:            nodeID,
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ETCDEndpoints:     []string{endpoint},
		ReplicationFactor: 2,
	})

	mux := http.NewServeMux()
	s.registerRoutes(mux)
	ts := httptest.NewServer(mux)
	s.publicAddr = ts.URL

	if err := s.connectEtcd(context.Background()); err != nil {
		ts.Close()
		t.Fatalf("connectEtcd for %s: %v", nodeID, err)
	}
	mustRegisterAndLoadServerState(t, s)

	t.Cleanup(func() {
		ts.Close()
		s.Close()
	})

	return s, ts
}

func mustRegisterAndLoadServerState(t *testing.T, s *Server) {
	t.Helper()

	if err := s.registerMember(context.Background()); err != nil {
		t.Fatalf("registerMember: %v", err)
	}
	if err := s.loadMembers(context.Background()); err != nil {
		t.Fatalf("loadMembers: %v", err)
	}
	if err := s.loadOfflineStates(context.Background()); err != nil {
		t.Fatalf("loadOfflineStates: %v", err)
	}
	if err := s.loadRouting(context.Background()); err != nil {
		t.Fatalf("loadRouting: %v", err)
	}
	if err := s.loadReplicaRepairStates(context.Background()); err != nil {
		t.Fatalf("loadReplicaRepairStates: %v", err)
	}
}

func putEtcdJSON(t *testing.T, client *clientv3.Client, key string, value any) {
	t.Helper()

	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal etcd value for %s: %v", key, err)
	}
	if _, err := client.Put(context.Background(), key, string(body)); err != nil {
		t.Fatalf("put etcd key %s: %v", key, err)
	}
}

func deleteEtcdKey(t *testing.T, client *clientv3.Client, key string) {
	t.Helper()

	if _, err := client.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete etcd key %s: %v", key, err)
	}
}

func mustAllocateURL(t *testing.T) url.URL {
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
