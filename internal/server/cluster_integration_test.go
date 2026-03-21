//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	integrationClusterNodes      = 4
	integrationDayCount          = 14
	integrationTotalEvents       = 10000
	integrationReplicationFactor = 3
	integrationIndexName         = "events"
)

type integrationCluster struct {
	coordinatorURL string
	nodeURLs       []string
}

type integrationDataset struct {
	Days         []string
	DocsByDay    map[string][]Document
	TotalEvents  int
	SharedToken  string
	SharedDocIDs map[string]string
	TargetDay    string
	TargetDocID  string
	TargetToken  string
}

type integrationRoutingSnapshot struct {
	Routing      map[string]RoutingEntryStats `json:"routing"`
	Members      map[string]NodeInfo          `json:"members"`
	ShardsPerDay int                          `json:"shards_per_day"`
}

type integrationHealthResponse struct {
	OK      bool                `json:"ok"`
	NodeID  string              `json:"node_id"`
	Members map[string]NodeInfo `json:"members"`
}

type integrationBulkResponse struct {
	OK      bool     `json:"ok"`
	Index   string   `json:"index"`
	Indexed int      `json:"indexed"`
	Failed  int      `json:"failed"`
	Errors  []string `json:"errors"`
}

type integrationSearchResponse struct {
	Index         string     `json:"index"`
	Days          []string   `json:"days"`
	Query         string     `json:"query"`
	K             int        `json:"k"`
	Hits          []ShardHit `json:"hits"`
	PartialErrors []string   `json:"partial_errors"`
}

func TestIntegration_ClusterDashboardAndSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker integration test in short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	cluster := startIntegrationCluster(t, ctx)
	waitForClusterMembership(t, cluster.nodeURLs, integrationClusterNodes)

	dataset := buildIntegrationDataset(time.Now().UTC())

	bootstrapDays(t, cluster.coordinatorURL, dataset.Days)
	waitForRoutingOnAllNodes(t, cluster.nodeURLs, len(dataset.Days)*enforcedShardsPerDay)

	ingestDataset(t, cluster.coordinatorURL, dataset)

	clusterHTML := fetchText(t, cluster.coordinatorURL+"/cluster")
	if !strings.Contains(clusterHTML, "Cluster dashboard") {
		t.Fatalf("expected cluster page to contain dashboard heading")
	}

	routing := waitForDashboardTotal(t, cluster.coordinatorURL, dataset.TotalEvents, len(dataset.Days)*enforcedShardsPerDay)
	validateDashboardSnapshot(t, routing, dataset)
	validateSharedTokenSearch(t, cluster.coordinatorURL, dataset)
	validateTargetedSearch(t, cluster.coordinatorURL, dataset)
}

func startIntegrationCluster(t *testing.T, ctx context.Context) integrationCluster {
	t.Helper()

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	net, err := tcnetwork.New(ctx, tcnetwork.WithAttachable(), tcnetwork.WithDriver("bridge"))
	if err != nil {
		t.Fatalf("create test network: %v", err)
	}
	testcontainers.CleanupNetwork(t, net)

	etcdContainer, err := testcontainers.Run(ctx, "quay.io/coreos/etcd:v3.6.9",
		testcontainers.WithExposedPorts("2379/tcp"),
		tcnetwork.WithNetwork([]string{"etcd"}, net),
		testcontainers.WithCmd(
			"/usr/local/bin/etcd",
			"--name=etcd",
			"--data-dir=/etcd-data",
			"--advertise-client-urls=http://etcd:2379",
			"--listen-client-urls=http://0.0.0.0:2379",
			"--initial-advertise-peer-urls=http://etcd:2380",
			"--listen-peer-urls=http://0.0.0.0:2380",
			"--initial-cluster=etcd=http://etcd:2380",
			"--initial-cluster-state=new",
		),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("2379/tcp").WithStartupTimeout(90*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, etcdContainer)
	if err != nil {
		t.Fatalf("start etcd container: %v", err)
	}

	imageTag := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	imageName := "elkgo-integration:" + imageTag
	nodeURLs := make([]string, 0, integrationClusterNodes)

	for i := 1; i <= integrationClusterNodes; i++ {
		alias := fmt.Sprintf("elkgo%d", i)
		nodeID := fmt.Sprintf("n%d", i)
		cmd := []string{
			"-mode=both",
			"-node-id=" + nodeID,
			"-listen=:8081",
			"-data=/app/data",
			"-etcd-endpoints=http://etcd:2379",
			fmt.Sprintf("-replication-factor=%d", integrationReplicationFactor),
		}
		opts := []testcontainers.ContainerCustomizer{
			testcontainers.WithEnv(map[string]string{
				"ELKGO_PUBLIC_ADDR": "http://" + alias + ":8081",
			}),
			testcontainers.WithExposedPorts("8081/tcp"),
			testcontainers.WithCmd(cmd...),
			tcnetwork.WithNetwork([]string{alias}, net),
			testcontainers.WithWaitStrategy(
				wait.ForHTTP("/healthz").WithPort("8081/tcp").WithStartupTimeout(2 * time.Minute),
			),
		}

		var node *testcontainers.DockerContainer
		if i == 1 {
			node, err = testcontainers.Run(ctx, "",
				append(opts, testcontainers.WithDockerfile(testcontainers.FromDockerfile{
					Context:        repoRoot,
					Dockerfile:     "Dockerfile",
					Repo:           "elkgo-integration",
					Tag:            imageTag,
					KeepImage:      false,
					BuildLogWriter: io.Discard,
				}))...,
			)
		} else {
			node, err = testcontainers.Run(ctx, imageName, opts...)
		}
		testcontainers.CleanupContainer(t, node)
		if err != nil {
			t.Fatalf("start app container %s: %v", nodeID, err)
		}

		baseURL, err := node.Endpoint(ctx, "http")
		if err != nil {
			t.Fatalf("get endpoint for %s: %v", nodeID, err)
		}
		nodeURLs = append(nodeURLs, baseURL)
	}

	return integrationCluster{
		coordinatorURL: nodeURLs[0],
		nodeURLs:       nodeURLs,
	}
}

func buildIntegrationDataset(reference time.Time) integrationDataset {
	days := lastNDays(reference, integrationDayCount)
	sharedToken := "integrationcanarysharedtoken"
	perDay := integrationTotalEvents / len(days)
	remainder := integrationTotalEvents % len(days)
	docsByDay := make(map[string][]Document, len(days))
	sharedDocIDs := make(map[string]string, len(days))

	targetDay := days[len(days)/2]
	targetToken := ""
	targetDocID := ""
	total := 0

	for idx, day := range days {
		dayCount := perDay
		if idx < remainder {
			dayCount++
		}

		dayCompact := strings.ReplaceAll(day, "-", "")
		dayToken := "integrationcanaryday" + dayCompact
		docID := "evt-" + dayCompact + "-canary"
		docs := make([]Document, 0, dayCount)
		docs = append(docs, Document{
			"id":        docID,
			"timestamp": day + "T12:00:00Z",
			"title":     "Integration canary " + day,
			"message":   sharedToken + " " + dayToken + " dashboard-search-validation",
			"tags":      []string{"integration", "canary", dayCompact},
		})

		for seq := 1; seq < dayCount; seq++ {
			docs = append(docs, Document{
				"id":        fmt.Sprintf("evt-%s-%04d", dayCompact, seq),
				"timestamp": day + fmt.Sprintf("T%02d:%02d:%02dZ", seq%24, (seq*7)%60, (seq*13)%60),
				"title":     fmt.Sprintf("Regular event %s #%d", day, seq),
				"message":   fmt.Sprintf("regular event for %s sequence %d", dayCompact, seq),
				"tags":      []string{"integration", "regular", dayCompact},
			})
		}

		docsByDay[day] = docs
		sharedDocIDs[docID] = day
		total += len(docs)

		if day == targetDay {
			targetToken = dayToken
			targetDocID = docID
		}
	}

	return integrationDataset{
		Days:         days,
		DocsByDay:    docsByDay,
		TotalEvents:  total,
		SharedToken:  sharedToken,
		SharedDocIDs: sharedDocIDs,
		TargetDay:    targetDay,
		TargetDocID:  targetDocID,
		TargetToken:  targetToken,
	}
}

func lastNDays(reference time.Time, count int) []string {
	base := reference.UTC().Truncate(24 * time.Hour)
	days := make([]string, 0, count)
	for offset := count - 1; offset >= 0; offset-- {
		days = append(days, base.AddDate(0, 0, -offset).Format("2006-01-02"))
	}
	return days
}

func waitForClusterMembership(t *testing.T, nodeURLs []string, expectedMembers int) {
	t.Helper()

	waitForCondition(t, 90*time.Second, 2*time.Second, "all nodes to observe full membership", func() (bool, error) {
		for _, nodeURL := range nodeURLs {
			var health integrationHealthResponse
			if err := getJSON(newIntegrationClient(15*time.Second), nodeURL+"/healthz", &health); err != nil {
				return false, err
			}
			if len(health.Members) != expectedMembers {
				return false, fmt.Errorf("%s sees %d members", nodeURL, len(health.Members))
			}
		}
		return true, nil
	})
}

func bootstrapDays(t *testing.T, coordinatorURL string, days []string) {
	t.Helper()

	client := newIntegrationClient(30 * time.Second)
	for _, day := range days {
		bootstrapURL := fmt.Sprintf("%s/admin/bootstrap?index=%s&day=%s&replication_factor=%d",
			coordinatorURL,
			url.QueryEscape(integrationIndexName),
			url.QueryEscape(day),
			integrationReplicationFactor,
		)
		waitForCondition(t, 30*time.Second, 1*time.Second, "bootstrap "+day, func() (bool, error) {
			req, err := http.NewRequest(http.MethodPost, bootstrapURL, nil)
			if err != nil {
				return false, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return false, err
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true, nil
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			return false, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		})
	}
}

func waitForRoutingOnAllNodes(t *testing.T, nodeURLs []string, expectedRoutes int) {
	t.Helper()

	waitForCondition(t, 90*time.Second, 2*time.Second, "routing to replicate across all nodes", func() (bool, error) {
		for _, nodeURL := range nodeURLs {
			var snapshot struct {
				Routing map[string]RoutingEntry `json:"routing"`
			}
			if err := getJSON(newIntegrationClient(20*time.Second), nodeURL+"/admin/routing", &snapshot); err != nil {
				return false, err
			}
			if len(snapshot.Routing) != expectedRoutes {
				return false, fmt.Errorf("%s has %d routes", nodeURL, len(snapshot.Routing))
			}
		}
		return true, nil
	})
}

func ingestDataset(t *testing.T, coordinatorURL string, dataset integrationDataset) {
	t.Helper()

	client := newIntegrationClient(2 * time.Minute)
	for _, day := range dataset.Days {
		docs := dataset.DocsByDay[day]
		resp := postNDJSON(t, client, coordinatorURL+"/bulk?index="+url.QueryEscape(integrationIndexName), docs)
		if resp.Indexed != len(docs) {
			t.Fatalf("expected %d indexed docs for %s, got %d", len(docs), day, resp.Indexed)
		}
		if resp.Failed != 0 || len(resp.Errors) != 0 {
			t.Fatalf("bulk ingest for %s failed: failed=%d errors=%v", day, resp.Failed, resp.Errors)
		}
	}
}

func waitForDashboardTotal(t *testing.T, coordinatorURL string, expectedTotal, expectedRoutes int) integrationRoutingSnapshot {
	t.Helper()

	var snapshot integrationRoutingSnapshot
	waitForCondition(t, 2*time.Minute, 5*time.Second, "dashboard routing totals", func() (bool, error) {
		if err := getJSON(newIntegrationClient(2*time.Minute), coordinatorURL+"/admin/routing?stats=1", &snapshot); err != nil {
			return false, err
		}
		if len(snapshot.Routing) != expectedRoutes {
			return false, fmt.Errorf("expected %d routes, got %d", expectedRoutes, len(snapshot.Routing))
		}
		total := 0
		for _, route := range snapshot.Routing {
			if route.CountError != "" {
				return false, fmt.Errorf("count error for %s/%s shard %d: %s", route.IndexName, route.Day, route.ShardID, route.CountError)
			}
			total += int(route.EventCount)
		}
		if total != expectedTotal {
			return false, fmt.Errorf("expected total %d, got %d", expectedTotal, total)
		}
		return true, nil
	})
	return snapshot
}

func validateDashboardSnapshot(t *testing.T, snapshot integrationRoutingSnapshot, dataset integrationDataset) {
	t.Helper()

	if len(snapshot.Members) != integrationClusterNodes {
		t.Fatalf("expected %d members, got %d", integrationClusterNodes, len(snapshot.Members))
	}
	if snapshot.ShardsPerDay != enforcedShardsPerDay {
		t.Fatalf("expected shards_per_day=%d, got %d", enforcedShardsPerDay, snapshot.ShardsPerDay)
	}

	daySet := make(map[string]struct{}, len(dataset.Days))
	total := 0
	for _, route := range snapshot.Routing {
		daySet[route.Day] = struct{}{}
		total += int(route.EventCount)
	}
	if total != dataset.TotalEvents {
		t.Fatalf("expected dashboard total %d, got %d", dataset.TotalEvents, total)
	}

	gotDays := make([]string, 0, len(daySet))
	for day := range daySet {
		gotDays = append(gotDays, day)
	}
	sort.Strings(gotDays)
	if !reflect.DeepEqual(gotDays, dataset.Days) {
		t.Fatalf("unexpected dashboard days: got %#v want %#v", gotDays, dataset.Days)
	}
}

func validateSharedTokenSearch(t *testing.T, coordinatorURL string, dataset integrationDataset) {
	t.Helper()

	searchURL := fmt.Sprintf(
		"%s/search?index=%s&day_from=%s&day_to=%s&q=%s&k=%d",
		coordinatorURL,
		url.QueryEscape(integrationIndexName),
		url.QueryEscape(dataset.Days[0]),
		url.QueryEscape(dataset.Days[len(dataset.Days)-1]),
		url.QueryEscape(dataset.SharedToken),
		len(dataset.Days)+8,
	)

	var search integrationSearchResponse
	if err := getJSON(newIntegrationClient(45*time.Second), searchURL, &search); err != nil {
		t.Fatalf("shared token search failed: %v", err)
	}
	if len(search.PartialErrors) != 0 {
		t.Fatalf("expected no partial errors, got %v", search.PartialErrors)
	}
	if !reflect.DeepEqual(search.Days, dataset.Days) {
		t.Fatalf("unexpected day range in search response: got %#v want %#v", search.Days, dataset.Days)
	}
	if len(search.Hits) != len(dataset.SharedDocIDs) {
		t.Fatalf("expected %d shared hits, got %d", len(dataset.SharedDocIDs), len(search.Hits))
	}

	got := make(map[string]string, len(search.Hits))
	for _, hit := range search.Hits {
		got[hit.DocID] = hit.Day
		if !strings.Contains(fmt.Sprint(hit.Source["message"]), dataset.SharedToken) {
			t.Fatalf("search hit %s does not contain shared token in message", hit.DocID)
		}
	}
	if !reflect.DeepEqual(got, dataset.SharedDocIDs) {
		t.Fatalf("unexpected shared search hits: got %#v want %#v", got, dataset.SharedDocIDs)
	}
}

func validateTargetedSearch(t *testing.T, coordinatorURL string, dataset integrationDataset) {
	t.Helper()

	searchURL := fmt.Sprintf(
		"%s/search?index=%s&day=%s&q=%s&k=10",
		coordinatorURL,
		url.QueryEscape(integrationIndexName),
		url.QueryEscape(dataset.TargetDay),
		url.QueryEscape(dataset.TargetToken),
	)

	var search integrationSearchResponse
	if err := getJSON(newIntegrationClient(30*time.Second), searchURL, &search); err != nil {
		t.Fatalf("targeted search failed: %v", err)
	}
	if len(search.PartialErrors) != 0 {
		t.Fatalf("expected no partial errors, got %v", search.PartialErrors)
	}
	if len(search.Hits) != 1 {
		t.Fatalf("expected 1 targeted hit, got %d", len(search.Hits))
	}

	hit := search.Hits[0]
	if hit.DocID != dataset.TargetDocID {
		t.Fatalf("expected targeted doc id %s, got %s", dataset.TargetDocID, hit.DocID)
	}
	if hit.Day != dataset.TargetDay {
		t.Fatalf("expected targeted hit day %s, got %s", dataset.TargetDay, hit.Day)
	}
	if !strings.Contains(fmt.Sprint(hit.Source["message"]), dataset.TargetToken) {
		t.Fatalf("targeted hit does not contain token %s", dataset.TargetToken)
	}
}

func waitForCondition(t *testing.T, timeout, interval time.Duration, description string, fn func() (bool, error)) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := fn()
		if ok {
			return
		}
		lastErr = err
		time.Sleep(interval)
	}
	if lastErr != nil {
		t.Fatalf("timed out waiting for %s: %v", description, lastErr)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func newIntegrationClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}

func getJSON(client *http.Client, requestURL string, out any) error {
	resp, err := client.Get(requestURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func fetchText(t *testing.T, requestURL string) string {
	t.Helper()

	resp, err := newIntegrationClient(30 * time.Second).Get(requestURL)
	if err != nil {
		t.Fatalf("fetch %s: %v", requestURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		t.Fatalf("fetch %s failed with status %d: %s", requestURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", requestURL, err)
	}
	return string(body)
}

func postNDJSON(t *testing.T, client *http.Client, requestURL string, docs []Document) integrationBulkResponse {
	t.Helper()

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			t.Fatalf("encode ndjson doc: %v", err)
		}
	}

	req, err := http.NewRequest(http.MethodPost, requestURL, &body)
	if err != nil {
		t.Fatalf("build bulk request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("bulk request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("bulk request returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var payload integrationBulkResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode bulk response: %v", err)
	}
	return payload
}
