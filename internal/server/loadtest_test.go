package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const loadTestCommonToken = "loadtestcommon"

type loadTestConfig struct {
	IndexCount        int `json:"index_count"`
	DayCount          int `json:"day_count"`
	DocsPerDay        int `json:"docs_per_day"`
	BatchSize         int `json:"batch_size"`
	SearchRequests    int `json:"search_requests"`
	SearchConcurrency int `json:"search_concurrency"`
}

type loadTestResult struct {
	GeneratedAt string                 `json:"generated_at"`
	Config      loadTestConfig         `json:"config"`
	Cluster     loadTestClusterShape   `json:"cluster"`
	Ingest      loadTestIngestMetrics  `json:"ingest"`
	Search      []loadTestSearchMetric `json:"search"`
}

type loadTestHistoryOptions struct {
	OutputFile string
	HistoryDir string
	CompareTo  string
}

type loadTestHistoryOutcome struct {
	OutputFile string
	ComparedTo string
	Comparison *loadTestComparison
}

type loadTestClusterShape struct {
	Nodes   int      `json:"nodes"`
	Indexes []string `json:"indexes"`
	Days    []string `json:"days"`
}

type loadTestIngestMetrics struct {
	Documents     int     `json:"documents"`
	DurationMS    float64 `json:"duration_ms"`
	DocsPerSecond float64 `json:"docs_per_second"`
	BatchSize     int     `json:"batch_size"`
}

type loadTestSearchScenario struct {
	Name    string
	Index   string
	DayFrom string
	DayTo   string
	Query   string
	K       int
}

type loadTestSearchMetric struct {
	Name              string   `json:"name"`
	Index             string   `json:"index"`
	Indexes           []string `json:"indexes,omitempty"`
	DayFrom           string   `json:"day_from"`
	DayTo             string   `json:"day_to"`
	Query             string   `json:"query"`
	K                 int      `json:"k"`
	Requests          int      `json:"requests"`
	Concurrency       int      `json:"concurrency"`
	TargetShards      int      `json:"target_shards"`
	AvgHits           float64  `json:"avg_hits"`
	DurationMS        float64  `json:"duration_ms"`
	RequestsPerSecond float64  `json:"requests_per_second"`
	P50MS             float64  `json:"p50_ms"`
	P95MS             float64  `json:"p95_ms"`
	MaxMS             float64  `json:"max_ms"`
}

type loadTestComparison struct {
	ComparedTo        string                     `json:"compared_to"`
	Comparable        bool                       `json:"comparable"`
	Notes             []string                   `json:"notes,omitempty"`
	IngestThroughput  loadTestMetricComparison   `json:"ingest_throughput"`
	SearchComparisons []loadTestSearchComparison `json:"search"`
}

type loadTestMetricComparison struct {
	Previous float64 `json:"previous"`
	Current  float64 `json:"current"`
	DeltaPct float64 `json:"delta_pct"`
	Status   string  `json:"status"`
}

type loadTestSearchComparison struct {
	Name              string                   `json:"name"`
	P50MS             loadTestMetricComparison `json:"p50_ms"`
	P95MS             loadTestMetricComparison `json:"p95_ms"`
	MaxMS             loadTestMetricComparison `json:"max_ms"`
	RequestsPerSecond loadTestMetricComparison `json:"requests_per_second"`
}

func TestLoadProfile_RealCluster(t *testing.T) {
	if strings.TrimSpace(os.Getenv("ELKGO_LOADTEST")) == "" {
		t.Skip("set ELKGO_LOADTEST=1 to run the real load profile")
	}

	cfg := loadTestConfig{
		IndexCount:        loadTestEnvInt(t, "ELKGO_LOADTEST_INDEXES", 2),
		DayCount:          loadTestEnvInt(t, "ELKGO_LOADTEST_DAYS", 3),
		DocsPerDay:        loadTestEnvInt(t, "ELKGO_LOADTEST_DOCS_PER_DAY", 2000),
		BatchSize:         loadTestEnvInt(t, "ELKGO_LOADTEST_BATCH_SIZE", 500),
		SearchRequests:    loadTestEnvInt(t, "ELKGO_LOADTEST_SEARCH_REQUESTS", 64),
		SearchConcurrency: loadTestEnvInt(t, "ELKGO_LOADTEST_SEARCH_CONCURRENCY", 8),
	}
	if cfg.DocsPerDay < enforcedShardsPerDay {
		t.Fatalf("ELKGO_LOADTEST_DOCS_PER_DAY must be at least %d to cover every shard", enforcedShardsPerDay)
	}

	indexes := loadTestIndexNames(cfg.IndexCount)
	days := loadTestDayRange(cfg.DayCount)
	coordinator, coordinatorTS := newNamedTestHTTPServer(t, "n1")
	replica, replicaTS := newNamedTestHTTPServer(t, "n2")

	members := map[string]NodeInfo{
		"n1": {ID: "n1", Addr: coordinatorTS.URL},
		"n2": {ID: "n2", Addr: replicaTS.URL},
	}
	for _, srv := range []*Server{coordinator, replica} {
		srv.membersMu.Lock()
		srv.members = members
		srv.membersMu.Unlock()
	}

	configureLoadTestRoutes(coordinator, replica, members, indexes, days)

	client := &http.Client{Timeout: 30 * time.Second}
	ingest := runLoadTestIngest(t, client, coordinatorTS.URL, indexes, days, cfg)

	scenarios := []loadTestSearchScenario{
		{
			Name:    "single_index_single_day",
			Index:   indexes[0],
			DayFrom: days[len(days)-1],
			DayTo:   days[len(days)-1],
			Query:   loadTestCommonToken,
			K:       20,
		},
		{
			Name:    "single_index_full_range",
			Index:   indexes[0],
			DayFrom: days[0],
			DayTo:   days[len(days)-1],
			Query:   loadTestCommonToken,
			K:       20,
		},
		{
			Name:    "all_indexes_full_range",
			Index:   "_all",
			DayFrom: days[0],
			DayTo:   days[len(days)-1],
			Query:   loadTestCommonToken,
			K:       20,
		},
	}

	searchMetrics := make([]loadTestSearchMetric, 0, len(scenarios))
	for _, scenario := range scenarios {
		searchMetrics = append(searchMetrics, runLoadTestSearchScenario(
			t,
			client,
			coordinatorTS.URL,
			scenario,
			indexes,
			cfg.SearchRequests,
			cfg.SearchConcurrency,
		))
	}

	result := loadTestResult{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Config:      cfg,
		Cluster: loadTestClusterShape{
			Nodes:   2,
			Indexes: indexes,
			Days:    days,
		},
		Ingest: ingest,
		Search: searchMetrics,
	}

	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("marshal load test result: %v", err)
	}
	t.Logf("LOADTEST_RESULT\n%s", string(body))

	outcome, err := persistLoadTestResult(result, loadTestHistoryOptions{
		OutputFile: strings.TrimSpace(os.Getenv("ELKGO_LOADTEST_OUTPUT_FILE")),
		HistoryDir: strings.TrimSpace(os.Getenv("ELKGO_LOADTEST_HISTORY_DIR")),
		CompareTo:  strings.TrimSpace(os.Getenv("ELKGO_LOADTEST_COMPARE_TO")),
	})
	if err != nil {
		t.Fatalf("persist load test result: %v", err)
	}
	if outcome.OutputFile != "" {
		t.Logf("LOADTEST_RESULT_FILE %s", outcome.OutputFile)
	}
	if outcome.Comparison != nil {
		comparisonBody, err := json.MarshalIndent(outcome.Comparison, "", "  ")
		if err != nil {
			t.Fatalf("marshal load test comparison: %v", err)
		}
		t.Logf("LOADTEST_COMPARISON\n%s", string(comparisonBody))
	}
}

func loadTestEnvInt(t *testing.T, key string, fallback int) int {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		t.Fatalf("invalid %s=%q", key, raw)
	}
	return value
}

func loadTestIndexNames(count int) []string {
	indexes := make([]string, 0, count)
	for i := 0; i < count; i++ {
		indexes = append(indexes, fmt.Sprintf("bench-%02d", i+1))
	}
	return indexes
}

func loadTestDayRange(count int) []string {
	start := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	days := make([]string, 0, count)
	for i := 0; i < count; i++ {
		days = append(days, start.AddDate(0, 0, i).Format("2006-01-02"))
	}
	return days
}

func configureLoadTestRoutes(primary, replica *Server, members map[string]NodeInfo, indexes, days []string) {
	nodes := make([]NodeInfo, 0, len(members))
	for _, nodeID := range []string{"n1", "n2"} {
		nodes = append(nodes, members[nodeID])
	}
	routes := generateRouting(nodes, enforcedShardsPerDay, 2)

	for _, indexName := range indexes {
		for _, day := range days {
			for shardID, replicas := range routes {
				setTestRoute(primary, indexName, day, shardID, replicas)
				setTestRoute(replica, indexName, day, shardID, replicas)
			}
		}
	}
}

func runLoadTestIngest(t *testing.T, client *http.Client, baseURL string, indexes, days []string, cfg loadTestConfig) loadTestIngestMetrics {
	t.Helper()

	totalDocs := 0
	start := time.Now()
	for _, indexName := range indexes {
		for _, day := range days {
			for offset := 0; offset < cfg.DocsPerDay; offset += cfg.BatchSize {
				count := cfg.BatchSize
				if remaining := cfg.DocsPerDay - offset; remaining < count {
					count = remaining
				}
				docs := buildLoadTestDocs(indexName, day, offset, count)
				if err := postLoadTestBulk(client, baseURL, indexName, docs); err != nil {
					t.Fatalf("bulk ingest failed for %s/%s offset=%d: %v", indexName, day, offset, err)
				}
				totalDocs += count
			}
		}
	}
	duration := time.Since(start)

	return loadTestIngestMetrics{
		Documents:     totalDocs,
		DurationMS:    duration.Seconds() * 1000,
		DocsPerSecond: float64(totalDocs) / duration.Seconds(),
		BatchSize:     cfg.BatchSize,
	}
}

func buildLoadTestDocs(indexName, day string, start, count int) []Document {
	docs := make([]Document, 0, count)
	dayToken := strings.ReplaceAll(day, "-", "")
	for i := 0; i < count; i++ {
		seq := start + i
		service := "api"
		if seq%2 == 1 {
			service = "worker"
		}
		docID := fmt.Sprintf("%s-%s-%06d", indexName, dayToken, seq)
		if seq < enforcedShardsPerDay {
			docID = loadTestDocIDForShard(seq, fmt.Sprintf("%s-%s-cover", indexName, dayToken))
		}
		docs = append(docs, Document{
			"id":        docID,
			"timestamp": fmt.Sprintf("%sT%02d:%02d:%02dZ", day, seq%24, (seq*7)%60, (seq*13)%60),
			"message":   fmt.Sprintf("%s %s %s seq-%d service-%s", loadTestCommonToken, indexName, dayToken, seq, service),
			"service":   service,
			"dataset":   indexName,
		})
	}
	return docs
}

func loadTestDocIDForShard(shardID int, prefix string) string {
	for i := 0; i < 100000; i++ {
		candidate := fmt.Sprintf("%s-%d", prefix, i)
		if keyToShard(candidate, enforcedShardsPerDay) == shardID {
			return candidate
		}
	}
	panic(fmt.Sprintf("could not find doc id for shard %d", shardID))
}

func postLoadTestBulk(client *http.Client, baseURL, indexName string, docs []Document) error {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}

	resp, err := client.Post(baseURL+"/bulk?index="+url.QueryEscape(indexName), "application/x-ndjson", buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, readAllAndCloseForLoadTest(resp))
	}

	var payload struct {
		OK      bool     `json:"ok"`
		Indexed int      `json:"indexed"`
		Failed  int      `json:"failed"`
		Errors  []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if !payload.OK || payload.Failed != 0 || payload.Indexed != len(docs) {
		return fmt.Errorf("unexpected bulk payload: %#v", payload)
	}
	return nil
}

func runLoadTestSearchScenario(
	t *testing.T,
	client *http.Client,
	baseURL string,
	scenario loadTestSearchScenario,
	availableIndexes []string,
	requests, concurrency int,
) loadTestSearchMetric {
	t.Helper()

	for i := 0; i < 3; i++ {
		if _, _, err := executeLoadTestSearch(client, baseURL, scenario); err != nil {
			t.Fatalf("warmup search failed for %s: %v", scenario.Name, err)
		}
	}

	jobs := make(chan struct{}, requests)
	for i := 0; i < requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	var totalHits atomic.Int64
	latencies := make(chan time.Duration, requests)
	errs := make(chan error, requests)
	start := time.Now()
	var wg sync.WaitGroup

	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				latency, hits, err := executeLoadTestSearch(client, baseURL, scenario)
				if err != nil {
					errs <- err
					continue
				}
				totalHits.Add(int64(hits))
				latencies <- latency
			}
		}()
	}

	wg.Wait()
	close(latencies)
	close(errs)
	duration := time.Since(start)

	if len(errs) > 0 {
		var messages []string
		for err := range errs {
			messages = append(messages, err.Error())
		}
		t.Fatalf("search scenario %s failed: %s", scenario.Name, strings.Join(messages, "; "))
	}

	samples := make([]time.Duration, 0, requests)
	for latency := range latencies {
		samples = append(samples, latency)
	}
	if len(samples) == 0 {
		t.Fatalf("search scenario %s produced no samples", scenario.Name)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	indexLabel := scenario.Index
	indexes := []string{scenario.Index}
	if normalizeSearchIndexScope(indexLabel) == "" {
		indexLabel = "_all"
		indexes = append([]string(nil), availableIndexes...)
	}

	targetIndexes := 1
	if normalizeSearchIndexScope(scenario.Index) == "" {
		targetIndexes = len(availableIndexes)
	}

	return loadTestSearchMetric{
		Name:              scenario.Name,
		Index:             indexLabel,
		Indexes:           indexes,
		DayFrom:           scenario.DayFrom,
		DayTo:             scenario.DayTo,
		Query:             scenario.Query,
		K:                 scenario.K,
		Requests:          len(samples),
		Concurrency:       concurrency,
		TargetShards:      targetIndexes * daySpanInclusive(scenario.DayFrom, scenario.DayTo) * enforcedShardsPerDay,
		AvgHits:           float64(totalHits.Load()) / float64(len(samples)),
		DurationMS:        duration.Seconds() * 1000,
		RequestsPerSecond: float64(len(samples)) / duration.Seconds(),
		P50MS:             percentileDuration(samples, 0.50).Seconds() * 1000,
		P95MS:             percentileDuration(samples, 0.95).Seconds() * 1000,
		MaxMS:             samples[len(samples)-1].Seconds() * 1000,
	}
}

func executeLoadTestSearch(client *http.Client, baseURL string, scenario loadTestSearchScenario) (time.Duration, int, error) {
	params := url.Values{}
	if strings.TrimSpace(scenario.Index) != "" {
		params.Set("index", scenario.Index)
	}
	params.Set("day_from", scenario.DayFrom)
	params.Set("day_to", scenario.DayTo)
	params.Set("q", scenario.Query)
	params.Set("k", strconv.Itoa(scenario.K))

	start := time.Now()
	resp, err := client.Get(baseURL + "/search?" + params.Encode())
	latency := time.Since(start)
	if err != nil {
		return latency, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return latency, 0, fmt.Errorf("status %d: %s", resp.StatusCode, readAllAndCloseForLoadTest(resp))
	}

	var payload struct {
		Hits          []ShardHit `json:"hits"`
		PartialErrors []string   `json:"partial_errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return latency, 0, err
	}
	if len(payload.PartialErrors) > 0 {
		return latency, 0, fmt.Errorf("partial errors: %s", strings.Join(payload.PartialErrors, "; "))
	}
	return latency, len(payload.Hits), nil
}

func percentileDuration(samples []time.Duration, percentile float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if percentile <= 0 {
		return samples[0]
	}
	if percentile >= 1 {
		return samples[len(samples)-1]
	}
	index := int(percentile * float64(len(samples)-1))
	return samples[index]
}

func daySpanInclusive(dayFrom, dayTo string) int {
	start, err := time.Parse("2006-01-02", dayFrom)
	if err != nil {
		return 0
	}
	end, err := time.Parse("2006-01-02", dayTo)
	if err != nil || end.Before(start) {
		return 0
	}
	return int(end.Sub(start).Hours()/24) + 1
}

func readAllAndCloseForLoadTest(resp *http.Response) string {
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return buf.String()
}

func TestPersistLoadTestResult_WritesHistoryAndComparesLatestComparable(t *testing.T) {
	historyDir := t.TempDir()

	incompatible := syntheticLoadTestResult(
		"2026-03-22T08:00:00Z",
		loadTestConfig{IndexCount: 3, DayCount: 7, DocsPerDay: 5000, BatchSize: 500, SearchRequests: 64, SearchConcurrency: 8},
		1000,
		map[string]loadTestSearchMetric{
			"single_index_single_day": {Name: "single_index_single_day", P50MS: 100, P95MS: 150, MaxMS: 200, RequestsPerSecond: 50},
		},
	)
	if err := writeLoadTestResultFile(filepath.Join(historyDir, "latest-incompatible.json"), incompatible); err != nil {
		t.Fatalf("write incompatible history: %v", err)
	}

	previousPath := filepath.Join(historyDir, "prior-comparable.json")
	previous := syntheticLoadTestResult(
		"2026-03-22T07:00:00Z",
		loadTestConfig{IndexCount: 2, DayCount: 7, DocsPerDay: 5000, BatchSize: 500, SearchRequests: 64, SearchConcurrency: 8},
		1000,
		map[string]loadTestSearchMetric{
			"single_index_single_day": {Name: "single_index_single_day", P50MS: 100, P95MS: 150, MaxMS: 200, RequestsPerSecond: 50},
			"single_index_full_range": {Name: "single_index_full_range", P50MS: 700, P95MS: 900, MaxMS: 1100, RequestsPerSecond: 12},
		},
	)
	if err := writeLoadTestResultFile(previousPath, previous); err != nil {
		t.Fatalf("write comparable history: %v", err)
	}

	current := syntheticLoadTestResult(
		"2026-03-22T09:00:00Z",
		loadTestConfig{IndexCount: 2, DayCount: 7, DocsPerDay: 5000, BatchSize: 500, SearchRequests: 64, SearchConcurrency: 8},
		900,
		map[string]loadTestSearchMetric{
			"single_index_single_day": {Name: "single_index_single_day", P50MS: 110, P95MS: 180, MaxMS: 230, RequestsPerSecond: 45},
			"single_index_full_range": {Name: "single_index_full_range", P50MS: 750, P95MS: 1050, MaxMS: 1250, RequestsPerSecond: 10},
		},
	)

	outcome, err := persistLoadTestResult(current, loadTestHistoryOptions{HistoryDir: historyDir})
	if err != nil {
		t.Fatalf("persist load test result: %v", err)
	}
	if outcome.OutputFile == "" {
		t.Fatalf("expected output file to be written")
	}
	if _, err := os.Stat(outcome.OutputFile); err != nil {
		t.Fatalf("stat output file: %v", err)
	}
	if outcome.ComparedTo != previousPath {
		t.Fatalf("expected compare path %s, got %s", previousPath, outcome.ComparedTo)
	}
	if outcome.Comparison == nil || !outcome.Comparison.Comparable {
		t.Fatalf("expected comparable result, got %#v", outcome.Comparison)
	}
	if outcome.Comparison.IngestThroughput.Status != "regressed" {
		t.Fatalf("expected ingest regression, got %#v", outcome.Comparison.IngestThroughput)
	}
	if len(outcome.Comparison.SearchComparisons) != 2 {
		t.Fatalf("expected two scenario comparisons, got %d", len(outcome.Comparison.SearchComparisons))
	}
	if outcome.Comparison.SearchComparisons[0].P95MS.Status != "regressed" {
		t.Fatalf("expected first scenario p95 regression, got %#v", outcome.Comparison.SearchComparisons[0].P95MS)
	}
}

func TestPersistLoadTestResult_ExplicitCompareReportsMismatch(t *testing.T) {
	tempDir := t.TempDir()
	comparePath := filepath.Join(tempDir, "baseline.json")
	previous := syntheticLoadTestResult(
		"2026-03-22T07:00:00Z",
		loadTestConfig{IndexCount: 3, DayCount: 7, DocsPerDay: 5000, BatchSize: 500, SearchRequests: 64, SearchConcurrency: 8},
		1000,
		map[string]loadTestSearchMetric{
			"single_index_single_day": {Name: "single_index_single_day", P50MS: 100, P95MS: 150, MaxMS: 200, RequestsPerSecond: 50},
		},
	)
	if err := writeLoadTestResultFile(comparePath, previous); err != nil {
		t.Fatalf("write compare file: %v", err)
	}

	outputPath := filepath.Join(tempDir, "current.json")
	current := syntheticLoadTestResult(
		"2026-03-22T09:00:00Z",
		loadTestConfig{IndexCount: 2, DayCount: 7, DocsPerDay: 5000, BatchSize: 500, SearchRequests: 64, SearchConcurrency: 8},
		950,
		map[string]loadTestSearchMetric{
			"single_index_single_day": {Name: "single_index_single_day", P50MS: 95, P95MS: 145, MaxMS: 190, RequestsPerSecond: 52},
		},
	)

	outcome, err := persistLoadTestResult(current, loadTestHistoryOptions{
		OutputFile: outputPath,
		CompareTo:  comparePath,
	})
	if err != nil {
		t.Fatalf("persist load test result with explicit compare: %v", err)
	}
	if outcome.OutputFile != outputPath {
		t.Fatalf("expected output file %s, got %s", outputPath, outcome.OutputFile)
	}
	if outcome.Comparison == nil {
		t.Fatalf("expected comparison to be present")
	}
	if outcome.Comparison.Comparable {
		t.Fatalf("expected mismatched comparison to be marked non-comparable")
	}
	if len(outcome.Comparison.Notes) == 0 {
		t.Fatalf("expected mismatch notes, got %#v", outcome.Comparison)
	}
}

func persistLoadTestResult(result loadTestResult, opts loadTestHistoryOptions) (loadTestHistoryOutcome, error) {
	outcome := loadTestHistoryOutcome{}
	comparePath := strings.TrimSpace(opts.CompareTo)

	if comparePath == "" && strings.TrimSpace(opts.HistoryDir) != "" {
		path, err := findLatestComparableLoadTestResult(strings.TrimSpace(opts.HistoryDir), result)
		if err != nil {
			return outcome, err
		}
		comparePath = path
	}

	outputFile := strings.TrimSpace(opts.OutputFile)
	if outputFile == "" && strings.TrimSpace(opts.HistoryDir) != "" {
		outputFile = filepath.Join(strings.TrimSpace(opts.HistoryDir), loadTestResultFilename(result))
	}
	if outputFile != "" {
		if err := writeLoadTestResultFile(outputFile, result); err != nil {
			return outcome, err
		}
		outcome.OutputFile = outputFile
	}

	if comparePath == "" {
		return outcome, nil
	}

	previous, err := readLoadTestResultFile(comparePath)
	if err != nil {
		return outcome, fmt.Errorf("read comparison result %s: %w", comparePath, err)
	}

	comparison := compareLoadTestResults(previous, result, comparePath)
	outcome.ComparedTo = comparePath
	outcome.Comparison = &comparison
	return outcome, nil
}

func findLatestComparableLoadTestResult(historyDir string, current loadTestResult) (string, error) {
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read history dir %s: %w", historyDir, err)
	}

	type historyEntry struct {
		path    string
		modTime time.Time
	}

	candidates := make([]historyEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return "", fmt.Errorf("stat history file %s: %w", entry.Name(), err)
		}
		candidates = append(candidates, historyEntry{
			path:    filepath.Join(historyDir, entry.Name()),
			modTime: info.ModTime(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime.After(candidates[j].modTime)
	})

	for _, candidate := range candidates {
		previous, err := readLoadTestResultFile(candidate.path)
		if err != nil {
			return "", fmt.Errorf("read history file %s: %w", candidate.path, err)
		}
		if loadTestResultsComparable(previous, current) {
			return candidate.path, nil
		}
	}
	return "", nil
}

func loadTestResultFilename(result loadTestResult) string {
	stamp, err := time.Parse(time.RFC3339, result.GeneratedAt)
	if err != nil {
		stamp = time.Now().UTC()
	}
	return fmt.Sprintf(
		"%s-i%d-d%d-e%d-q%d-c%d.json",
		stamp.UTC().Format("20060102T150405Z"),
		result.Config.IndexCount,
		result.Config.DayCount,
		result.Config.DocsPerDay,
		result.Config.SearchRequests,
		result.Config.SearchConcurrency,
	)
}

func writeLoadTestResultFile(path string, result loadTestResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create result dir: %w", err)
	}
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result file: %w", err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		return fmt.Errorf("write result file %s: %w", path, err)
	}
	return nil
}

func readLoadTestResultFile(path string) (loadTestResult, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return loadTestResult{}, err
	}
	var result loadTestResult
	if err := json.Unmarshal(body, &result); err != nil {
		return loadTestResult{}, fmt.Errorf("decode load test result: %w", err)
	}
	return result, nil
}

func compareLoadTestResults(previous, current loadTestResult, comparedTo string) loadTestComparison {
	comparison := loadTestComparison{
		ComparedTo: comparedTo,
		Comparable: false,
	}
	if !loadTestResultsComparable(previous, current) {
		comparison.Notes = []string{"configuration differs; comparison is informational only"}
		return comparison
	}

	comparison.Comparable = true
	comparison.IngestThroughput = makeLoadTestMetricComparison(previous.Ingest.DocsPerSecond, current.Ingest.DocsPerSecond, true)

	previousSearch := make(map[string]loadTestSearchMetric, len(previous.Search))
	for _, metric := range previous.Search {
		previousSearch[metric.Name] = metric
	}

	for _, metric := range current.Search {
		prevMetric, ok := previousSearch[metric.Name]
		if !ok {
			comparison.Notes = append(comparison.Notes, "missing prior scenario "+metric.Name)
			continue
		}
		comparison.SearchComparisons = append(comparison.SearchComparisons, loadTestSearchComparison{
			Name:              metric.Name,
			P50MS:             makeLoadTestMetricComparison(prevMetric.P50MS, metric.P50MS, false),
			P95MS:             makeLoadTestMetricComparison(prevMetric.P95MS, metric.P95MS, false),
			MaxMS:             makeLoadTestMetricComparison(prevMetric.MaxMS, metric.MaxMS, false),
			RequestsPerSecond: makeLoadTestMetricComparison(prevMetric.RequestsPerSecond, metric.RequestsPerSecond, true),
		})
	}

	if len(comparison.SearchComparisons) == 0 {
		comparison.Comparable = false
		comparison.Notes = append(comparison.Notes, "no overlapping search scenarios found")
	}
	return comparison
}

func loadTestResultsComparable(previous, current loadTestResult) bool {
	return previous.Config == current.Config && previous.Cluster.Nodes == current.Cluster.Nodes
}

func makeLoadTestMetricComparison(previous, current float64, higherIsBetter bool) loadTestMetricComparison {
	deltaPct := loadTestDeltaPct(previous, current)
	status := "stable"
	if math.Abs(deltaPct) > 1 {
		switch {
		case higherIsBetter && deltaPct > 0:
			status = "improved"
		case higherIsBetter && deltaPct < 0:
			status = "regressed"
		case !higherIsBetter && deltaPct > 0:
			status = "regressed"
		case !higherIsBetter && deltaPct < 0:
			status = "improved"
		}
	}
	return loadTestMetricComparison{
		Previous: previous,
		Current:  current,
		DeltaPct: deltaPct,
		Status:   status,
	}
}

func loadTestDeltaPct(previous, current float64) float64 {
	if previous == 0 {
		if current == 0 {
			return 0
		}
		return 100
	}
	return ((current - previous) / previous) * 100
}

func syntheticLoadTestResult(generatedAt string, cfg loadTestConfig, docsPerSecond float64, search map[string]loadTestSearchMetric) loadTestResult {
	metrics := make([]loadTestSearchMetric, 0, len(search))
	names := make([]string, 0, len(search))
	for name := range search {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metrics = append(metrics, search[name])
	}
	return loadTestResult{
		GeneratedAt: generatedAt,
		Config:      cfg,
		Cluster: loadTestClusterShape{
			Nodes:   2,
			Indexes: loadTestIndexNames(cfg.IndexCount),
			Days:    loadTestDayRange(cfg.DayCount),
		},
		Ingest: loadTestIngestMetrics{
			Documents:     cfg.IndexCount * cfg.DayCount * cfg.DocsPerDay,
			DocsPerSecond: docsPerSecond,
			BatchSize:     cfg.BatchSize,
		},
		Search: metrics,
	}
}
