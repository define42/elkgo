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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	dockerclient "github.com/docker/docker/client"
	testcontainers "github.com/testcontainers/testcontainers-go"
)

const crashRepairToken = "integrationcrashrepairtoken"

func TestIntegration_CrashRestartRepairsMissedFollowerWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker integration test in short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	cluster := startIntegrationCluster(t, ctx)
	waitForClusterMembership(t, cluster.nodeURLs, integrationClusterNodes)

	day := time.Now().UTC().Format("2006-01-02")
	bootstrapDays(t, cluster.coordinatorURL, []string{day})
	waitForRoutingOnAllNodes(t, cluster.nodeURLs, enforcedShardsPerDay)

	crashedNodeIndex := 1
	crashedNodeID := "n2"
	route := findFollowerRouteForNode(t, cluster.coordinatorURL, day, crashedNodeID)

	killIntegrationNode(t, ctx, cluster, crashedNodeIndex)
	waitForClusterMembership(t, []string{
		cluster.nodeURLs[0],
		cluster.nodeURLs[2],
		cluster.nodeURLs[3],
	}, integrationClusterNodes-1)

	docID := findDocIDForShard(t, route.ShardID, "crash-repair")
	doc := Document{
		"id":        docID,
		"timestamp": day + "T14:15:00Z",
		"message":   "hard crash repair validation " + crashRepairToken,
		"service":   "integration",
		"level":     "warn",
	}

	indexIntegrationDocument(t, cluster.coordinatorURL, integrationIndexName, doc)
	waitForReplicaDocumentOnNode(t, nodeURLForIntegrationNode(cluster, route.Replicas[0]), route, docID, crashRepairToken)

	restartIntegrationNode(t, ctx, &cluster, crashedNodeIndex)
	waitForClusterMembership(t, cluster.nodeURLs, integrationClusterNodes)
	waitForReplicaDocumentOnNode(t, cluster.nodeURLs[crashedNodeIndex], route, docID, crashRepairToken)
}

func killIntegrationNode(t *testing.T, ctx context.Context, cluster integrationCluster, nodeIndex int) {
	t.Helper()

	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("create docker client: %v", err)
	}
	defer cli.Close()

	containerID := cluster.nodeContainers[nodeIndex].GetContainerID()
	if containerID == "" {
		t.Fatalf("missing container id for node %d", nodeIndex+1)
	}
	if err := cli.ContainerKill(ctx, containerID, "SIGKILL"); err != nil {
		t.Fatalf("kill node %d container: %v", nodeIndex+1, err)
	}
}

func findFollowerRouteForNode(t *testing.T, coordinatorURL, day, nodeID string) RoutingEntry {
	t.Helper()

	var snapshot struct {
		Routing map[string]RoutingEntry `json:"routing"`
	}
	if err := getJSON(newIntegrationClient(30*time.Second), coordinatorURL+"/admin/routing", &snapshot); err != nil {
		t.Fatalf("load routing snapshot: %v", err)
	}

	keys := make([]string, 0, len(snapshot.Routing))
	for key := range snapshot.Routing {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		route := snapshot.Routing[key]
		if route.Day != day {
			continue
		}
		if len(route.Replicas) == 0 || route.Replicas[0] == nodeID {
			continue
		}
		if routeHasReplica(route, nodeID) {
			return route
		}
	}

	t.Fatalf("could not find follower route for node %s on %s", nodeID, day)
	return RoutingEntry{}
}

func indexIntegrationDocument(t *testing.T, coordinatorURL, indexName string, doc Document) {
	t.Helper()

	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal integration document: %v", err)
	}

	requestURL := fmt.Sprintf("%s/index?index=%s", coordinatorURL, url.QueryEscape(indexName))
	req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build integration index request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := newIntegrationClient(30 * time.Second).Do(req)
	if err != nil {
		t.Fatalf("send integration index request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("integration index failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
}

func waitForReplicaDocumentOnNode(t *testing.T, nodeURL string, route RoutingEntry, docID, token string) {
	t.Helper()

	waitForCondition(t, 2*time.Minute, 2*time.Second, "repaired doc on restarted replica", func() (bool, error) {
		body, err := json.Marshal(FetchDocsRequest{
			IndexName: route.IndexName,
			Day:       route.Day,
			ShardID:   route.ShardID,
			DocIDs:    []string{docID},
		})
		if err != nil {
			return false, err
		}

		req, err := http.NewRequest(http.MethodPost, nodeURL+"/internal/fetch_docs", bytes.NewReader(body))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := newIntegrationClient(30 * time.Second).Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			return false, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		var payload FetchDocsResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return false, err
		}
		if len(payload.Docs) != 1 {
			return false, fmt.Errorf("expected 1 repaired doc, got %d", len(payload.Docs))
		}
		if payload.Docs[0].DocID != docID {
			return false, fmt.Errorf("unexpected repaired doc id %s", payload.Docs[0].DocID)
		}
		if !strings.Contains(fmt.Sprint(payload.Docs[0].Source["message"]), token) {
			return false, fmt.Errorf("repaired doc missing token %s", token)
		}
		return true, nil
	})
}

func nodeURLForIntegrationNode(cluster integrationCluster, nodeID string) string {
	if strings.HasPrefix(nodeID, "n") {
		if ordinal, err := strconv.Atoi(strings.TrimPrefix(nodeID, "n")); err == nil && ordinal > 0 && ordinal <= len(cluster.nodeURLs) {
			return cluster.nodeURLs[ordinal-1]
		}
	}
	return ""
}
