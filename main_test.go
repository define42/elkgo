package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"net/url"

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
	const key = "ELKGO_MAIN_TEST_ENV"
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

func TestRun_ReturnsValidationAndListenErrors(t *testing.T) {
	if err := run([]string{"-etcd-endpoints=   "}); err == nil || !strings.Contains(err.Error(), "at least one etcd endpoint is required") {
		t.Fatalf("expected missing endpoint error, got %v", err)
	}

	endpoint := startEmbeddedEtcdForMain(t)
	err := run([]string{
		"-node-id=test-main",
		"-listen=bad-listen-address",
		"-public-addr=http://127.0.0.1:18101",
		"-data=" + t.TempDir(),
		"-etcd-endpoints=" + endpoint,
		"-replication-factor=1",
	})
	if err == nil {
		t.Fatalf("expected run to fail on invalid listen address")
	}
	if !strings.Contains(err.Error(), "listen") && !strings.Contains(err.Error(), "missing port") {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func startEmbeddedEtcdForMain(t *testing.T) string {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = fmt.Sprintf("main-etcd-%d", time.Now().UnixNano())
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(cfg.Dir, "etcd.log")}

	clientURL := allocateMainURL(t)
	peerURL := allocateMainURL(t)
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

func allocateMainURL(t *testing.T) url.URL {
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
