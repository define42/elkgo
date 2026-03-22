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
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
)

const (
	drainRestartIntegrationDocs      = 1000
	drainRestartIntegrationBatchSize = 250
	drainRestartIntegrationDelay     = 100 * time.Millisecond
	drainRestartSignalAfterDocs      = 500
)

func TestIntegration_DrainRestartDuringIngestKeepsEventCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker integration test in short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	cluster := startIntegrationCluster(t, ctx)
	waitForClusterMembership(t, cluster.nodeURLs, integrationClusterNodes)

	day := time.Now().UTC().Format("2006-01-02")
	dataset := buildDrainRestartIntegrationDataset(day, drainRestartIntegrationDocs)

	bootstrapDays(t, cluster.coordinatorURL, dataset.Days)
	waitForRoutingOnAllNodes(t, cluster.nodeURLs, len(dataset.Days)*enforcedShardsPerDay)

	signalCh := make(chan struct{}, 1)
	ingestErrCh := make(chan error, 1)
	go func() {
		ingestErrCh <- ingestDatasetSlowly(
			ctx,
			newIntegrationClient(2*time.Minute),
			cluster.coordinatorURL,
			dataset,
			drainRestartIntegrationBatchSize,
			drainRestartIntegrationDelay,
			drainRestartSignalAfterDocs,
			signalCh,
		)
	}()

	select {
	case <-signalCh:
	case err := <-ingestErrCh:
		if err != nil {
			t.Fatalf("ingest failed before drain window: %v", err)
		}
		t.Fatalf("ingest completed before drain window opened")
	case <-time.After(2 * time.Minute):
		t.Fatalf("timed out waiting for ingest to reach drain window")
	}

	drainedNodeID := "n2"
	drainedNodeIndex := 1
	setNodeDrain(t, cluster.coordinatorURL, drainedNodeID, true)
	waitForNodeToDrain(t, cluster.nodeURLs[drainedNodeIndex], drainedNodeID, dataset)

	restartIntegrationNode(t, ctx, &cluster, drainedNodeIndex)
	waitForClusterMembership(t, cluster.nodeURLs, integrationClusterNodes)

	setNodeDrain(t, cluster.coordinatorURL, drainedNodeID, false)
	waitForRoutingOnAllNodes(t, cluster.nodeURLs, len(dataset.Days)*enforcedShardsPerDay)
	waitForRebalancedShardsOnNode(t, cluster.nodeURLs[drainedNodeIndex], drainedNodeID, dataset)
	waitForRebalancedShardCountsOnNode(t, cluster, drainedNodeID, dataset)

	if err := <-ingestErrCh; err != nil {
		t.Fatalf("ingest during drain/restart failed: %v", err)
	}

	waitForDashboardTotal(t, cluster.coordinatorURL, dataset.TotalEvents, len(dataset.Days)*enforcedShardsPerDay)
}

func buildDrainRestartIntegrationDataset(day string, totalDocs int) integrationDataset {
	base, err := time.Parse("2006-01-02", day)
	if err != nil {
		base = time.Now().UTC().Truncate(24 * time.Hour)
	}
	base = base.UTC()

	docs := make([]Document, 0, totalDocs)
	for i := 0; i < totalDocs; i++ {
		timestamp := base.Add(time.Duration((i*11)%86400) * time.Second).Format(time.RFC3339)
		docs = append(docs, Document{
			"id":        fmt.Sprintf("drain-restart-%06d", i+1),
			"timestamp": timestamp,
			"message":   fmt.Sprintf("drain restart integration event %d", i+1),
			"service":   "integration",
			"level":     "info",
		})
	}

	return integrationDataset{
		Days:        []string{day},
		DocsByDay:   map[string][]Document{day: docs},
		TotalEvents: totalDocs,
	}
}

func ingestDatasetSlowly(ctx context.Context, client *http.Client, coordinatorURL string, dataset integrationDataset, batchSize int, batchDelay time.Duration, signalAfterDocs int, signalCh chan<- struct{}) error {
	for _, day := range dataset.Days {
		docs := dataset.DocsByDay[day]
		indexed := 0
		for start := 0; start < len(docs); start += batchSize {
			end := start + batchSize
			if end > len(docs) {
				end = len(docs)
			}

			resp, err := postIntegrationNDJSON(ctx, client, coordinatorURL+"/bulk?index="+url.QueryEscape(integrationIndexName), docs[start:end])
			if err != nil {
				return err
			}
			if resp.Indexed != end-start {
				return fmt.Errorf("expected %d indexed docs for batch %d-%d, got %d", end-start, start, end-1, resp.Indexed)
			}
			if resp.Failed != 0 || len(resp.Errors) != 0 {
				return fmt.Errorf("bulk ingest for batch %d-%d failed: failed=%d errors=%v", start, end-1, resp.Failed, resp.Errors)
			}

			indexed += end - start
			if indexed >= signalAfterDocs {
				select {
				case signalCh <- struct{}{}:
				default:
				}
			}

			if batchDelay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(batchDelay):
				}
			}
		}
	}
	return nil
}

func postIntegrationNDJSON(ctx context.Context, client *http.Client, requestURL string, docs []Document) (integrationBulkResponse, error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return integrationBulkResponse{}, err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, &body)
	if err != nil {
		return integrationBulkResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := client.Do(req)
	if err != nil {
		return integrationBulkResponse{}, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
	if err != nil {
		return integrationBulkResponse{}, err
	}
	if resp.StatusCode/100 != 2 {
		return integrationBulkResponse{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var payload integrationBulkResponse
	if err := json.Unmarshal(responseBody, &payload); err != nil {
		return integrationBulkResponse{}, err
	}
	return payload, nil
}

func restartIntegrationNode(t *testing.T, ctx context.Context, cluster *integrationCluster, nodeIndex int) {
	t.Helper()

	node := cluster.nodeContainers[nodeIndex]
	stopTimeout := 30 * time.Second
	if err := node.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop node %d: %v", nodeIndex+1, err)
	}
	if err := node.Start(ctx); err != nil {
		t.Fatalf("restart node %d: %v", nodeIndex+1, err)
	}

	baseURL, err := node.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("endpoint for restarted node %d: %v", nodeIndex+1, err)
	}
	cluster.nodeURLs[nodeIndex] = baseURL

	waitForCondition(t, 2*time.Minute, 2*time.Second, fmt.Sprintf("node n%d health after restart", nodeIndex+1), func() (bool, error) {
		resp, err := newIntegrationClient(15 * time.Second).Get(baseURL + "/healthz")
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			return false, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return true, nil
	})
}
