package testdatagen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	DefaultIndexName       = "events"
	DefaultDayCount        = 7
	DefaultTotalEvents     = 70000
	DefaultEventsPerDay    = DefaultTotalEvents / DefaultDayCount
	DefaultBulkBatchSize   = 1000
	DefaultMarkerVersion   = "v3"
	defaultHTTPTimeout     = 8 * time.Second
	defaultBulkTimeout     = 5 * time.Minute
	defaultWaitTimeout     = 2 * time.Minute
	defaultPollInterval    = 1 * time.Second
	defaultElectionPath    = "/distsearch/admin/test-data"
	defaultMarkerPrefix    = "/distsearch/admin/test-data-loaded/"
	defaultEtcdDialTimeout = 5 * time.Second
)

type Document map[string]any

type Config struct {
	ServerURL         string
	ETCDEndpoints     []string
	IndexName         string
	ReplicationFactor int
	WaitForMembers    int
	BulkBatchSize     int
	HTTPTimeout       time.Duration
	BulkTimeout       time.Duration
	WaitTimeout       time.Duration
	PollInterval      time.Duration
	MarkerVersion     string
}

type Generator struct {
	cfg    Config
	client *http.Client
	etcd   *clientv3.Client
}

func New(cfg Config) *Generator {
	cfg = cfg.withDefaults()
	return &Generator{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

func (g *Generator) Run(ctx context.Context) error {
	if g.cfg.ServerURL == "" {
		return fmt.Errorf("server URL is required")
	}
	if len(g.cfg.ETCDEndpoints) == 0 {
		return fmt.Errorf("at least one etcd endpoint is required")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   append([]string(nil), g.cfg.ETCDEndpoints...),
		DialTimeout: defaultEtcdDialTimeout,
	})
	if err != nil {
		return err
	}
	g.etcd = cli
	defer g.Close()

	if err := g.waitForCluster(ctx); err != nil {
		return err
	}

	sess, err := concurrency.NewSession(g.etcd)
	if err != nil {
		return fmt.Errorf("test data session failed: %w", err)
	}
	defer sess.Close()

	elect := concurrency.NewElection(sess, defaultElectionPath)
	campaignCtx, cancel := context.WithTimeout(ctx, g.cfg.WaitTimeout)
	defer cancel()
	if err := elect.Campaign(campaignCtx, g.cfg.ServerURL); err != nil {
		return fmt.Errorf("test data leadership failed: %w", err)
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	days := testDataDays(time.Now().UTC())
	seeded := 0
	for _, day := range days {
		markerKey := g.markerKey(day)
		if resp, err := g.etcd.Get(ctx, markerKey); err == nil && len(resp.Kvs) > 0 {
			continue
		}

		if err := g.bootstrapDay(ctx, day); err != nil {
			return fmt.Errorf("bootstrap test data routing failed for day %s: %w", day, err)
		}

		indexed, err := g.postDocumentsInBatches(ctx, g.cfg.ServerURL+"/bulk?index="+url.QueryEscape(g.cfg.IndexName), testDataDocuments(day), g.cfg.BulkBatchSize)
		if err != nil {
			return fmt.Errorf("seed test bulk ingest failed for day %s: %w", day, err)
		}
		if _, err := g.etcd.Put(ctx, markerKey, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("mark test data loaded failed for day %s: %w", day, err)
		}
		seeded += indexed
	}

	log.Printf("test data ready index=%s from=%s to=%s seeded=%d total=%d", g.cfg.IndexName, days[0], days[len(days)-1], seeded, DefaultTotalEvents)
	return nil
}

func (g *Generator) Close() error {
	if g.etcd == nil {
		return nil
	}
	err := g.etcd.Close()
	g.etcd = nil
	return err
}

func (cfg Config) withDefaults() Config {
	cfg.ServerURL = strings.TrimRight(strings.TrimSpace(cfg.ServerURL), "/")
	cfg.IndexName = strings.TrimSpace(cfg.IndexName)
	if cfg.IndexName == "" {
		cfg.IndexName = DefaultIndexName
	}
	if cfg.ReplicationFactor < 1 {
		cfg.ReplicationFactor = 1
	}
	if cfg.WaitForMembers < 1 {
		cfg.WaitForMembers = cfg.ReplicationFactor
	}
	if cfg.BulkBatchSize < 1 {
		cfg.BulkBatchSize = DefaultBulkBatchSize
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaultHTTPTimeout
	}
	if cfg.BulkTimeout <= 0 {
		cfg.BulkTimeout = defaultBulkTimeout
	}
	if cfg.WaitTimeout <= 0 {
		cfg.WaitTimeout = defaultWaitTimeout
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if strings.TrimSpace(cfg.MarkerVersion) == "" {
		cfg.MarkerVersion = DefaultMarkerVersion
	}
	return cfg
}

func (g *Generator) waitForCluster(ctx context.Context) error {
	deadline := time.Now().Add(g.cfg.WaitTimeout)
	lastCount := 0
	var lastErr error

	for {
		var health struct {
			OK      bool           `json:"ok"`
			NodeID  string         `json:"node_id"`
			Members map[string]any `json:"members"`
		}
		lastErr = g.getJSON(ctx, g.cfg.ServerURL+"/healthz", &health)
		if lastErr == nil {
			lastCount = len(health.Members)
			if health.OK && lastCount >= g.cfg.WaitForMembers {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for %d members: last error: %w", g.cfg.WaitForMembers, lastErr)
			}
			return fmt.Errorf("timed out waiting for %d members: saw %d", g.cfg.WaitForMembers, lastCount)
		}
		if err := sleepWithContext(ctx, g.cfg.PollInterval); err != nil {
			return err
		}
	}
}

func (g *Generator) bootstrapDay(ctx context.Context, day string) error {
	bootstrapURL := fmt.Sprintf(
		"%s/admin/bootstrap?index=%s&day=%s&replication_factor=%d",
		g.cfg.ServerURL,
		url.QueryEscape(g.cfg.IndexName),
		url.QueryEscape(day),
		g.cfg.ReplicationFactor,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, bootstrapURL, nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (g *Generator) postDocumentsInBatches(ctx context.Context, ingestURL string, docs []Document, batchSize int) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	if batchSize < 1 {
		batchSize = len(docs)
	}

	indexed := 0
	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		var resp struct {
			OK      bool     `json:"ok"`
			Indexed int      `json:"indexed"`
			Failed  int      `json:"failed"`
			Errors  []string `json:"errors"`
		}
		if err := g.postNDJSONWithTimeout(ctx, ingestURL, docs[start:end], g.cfg.BulkTimeout, &resp); err != nil {
			return indexed, err
		}
		if resp.Failed > 0 {
			return indexed, fmt.Errorf("batch %d-%d partially failed: indexed=%d failed=%d errors=%v", start, end-1, resp.Indexed, resp.Failed, resp.Errors)
		}
		indexed += resp.Indexed
	}

	return indexed, nil
}

func (g *Generator) postNDJSONWithTimeout(ctx context.Context, targetURL string, docs []Document, timeout time.Duration, out any) error {
	client := &http.Client{Timeout: timeout}
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (g *Generator) getJSON(ctx context.Context, targetURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
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

func (g *Generator) markerKey(day string) string {
	return defaultMarkerPrefix + g.cfg.MarkerVersion + "/" + g.cfg.IndexName + "/" + day
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
