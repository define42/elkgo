package server

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

const (
	defaultSearchShardConcurrency = 32
	maxSearchShardConcurrency     = 128
)

type searchHitRef struct {
	Target RoutingEntry
	DocID  string
	Score  float64
}

type searchHitMinHeap []searchHitRef

func (h searchHitMinHeap) Len() int { return len(h) }

func (h searchHitMinHeap) Less(i, j int) bool {
	if h[i].Score == h[j].Score {
		return h[i].DocID > h[j].DocID
	}
	return h[i].Score < h[j].Score
}

func (h searchHitMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *searchHitMinHeap) Push(x any) {
	*h = append(*h, x.(searchHitRef))
}

func (h *searchHitMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if !s.tryAcquireSearchAdmission() {
		http.Error(w, "too many concurrent search requests", http.StatusTooManyRequests)
		return
	}
	defer s.releaseSearchAdmission()

	indexName := normalizeSearchIndexScope(r.URL.Query().Get("index"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	k := 10
	if raw := r.URL.Query().Get("k"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 1000 {
			k = parsed
		}
	}
	days, err := resolveSearchDays(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	targets := s.collectSearchTargets(indexName, days)
	if len(targets) == 0 {
		http.Error(w, "routing not initialized for requested index/day range", http.StatusServiceUnavailable)
		return
	}

	refs, partial := s.collectTopSearchRefs(r.Context(), targets, q, k)
	hits, fetchErrors := s.fetchSearchHits(r.Context(), refs)
	partial = append(partial, fetchErrors...)

	writeJSON(w, http.StatusOK, map[string]any{
		"index":          searchIndexLabel(indexName),
		"indexes":        targetIndexNames(targets),
		"days":           days,
		"query":          q,
		"k":              k,
		"hits":           hits,
		"partial_errors": partial,
		"shards_per_day": maxShardCountFromTargets(targets, s.defaultShardsPerDay),
	})
}

func (s *Server) collectTopSearchRefs(ctx context.Context, targets []RoutingEntry, q string, k int) ([]searchHitRef, []string) {
	type result struct {
		refs []searchHitRef
		err  error
	}

	results := make(chan result, len(targets))
	sem := make(chan struct{}, effectiveSearchShardConcurrency(len(targets)))
	var wg sync.WaitGroup

	for _, target := range targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			refs, err := s.searchShardRefs(ctx, target, q, k)
			results <- result{refs: refs, err: err}
		}()
	}

	wg.Wait()
	close(results)

	var top searchHitMinHeap
	heap.Init(&top)
	partial := make([]string, 0)

	for res := range results {
		if res.err != nil {
			partial = append(partial, res.err.Error())
			continue
		}
		for _, ref := range res.refs {
			if top.Len() < k {
				heap.Push(&top, ref)
				continue
			}
			if compareSearchRefs(ref, top[0]) <= 0 {
				continue
			}
			top[0] = ref
			heap.Fix(&top, 0)
		}
	}

	out := make([]searchHitRef, top.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&top).(searchHitRef)
	}
	return out, partial
}

func (s *Server) searchShardRefs(ctx context.Context, target RoutingEntry, q string, k int) ([]searchHitRef, error) {
	tried := make(map[string]struct{}, len(target.Replicas))
	errorsOut := make([]string, 0, len(target.Replicas))
	routeKey := routingMapKey(target.IndexName, target.Day, target.ShardID)

	for attempt := 0; attempt < len(target.Replicas); attempt++ {
		replicaNodeID, addr, err := s.pickReplicaForRoute(ctx, target, tried)
		if err != nil {
			if len(errorsOut) == 0 {
				return nil, err
			}
			break
		}
		tried[replicaNodeID] = struct{}{}

		var resp SearchShardResponse
		err = s.postJSON(ctx, addr+"/internal/search_shard", SearchShardRequest{
			IndexName: target.IndexName,
			Day:       target.Day,
			ShardID:   target.ShardID,
			Query:     q,
			K:         k * 2,
			FetchDocs: false,
		}, &resp)
		if err == nil {
			refs := make([]searchHitRef, 0, len(resp.Hits))
			for _, hit := range resp.Hits {
				refs = append(refs, searchHitRef{
					Target: target,
					DocID:  hit.DocID,
					Score:  hit.Score,
				})
			}
			return refs, nil
		}

		s.invalidateReplica(routeKey, replicaNodeID)
		errorsOut = append(errorsOut, replicaNodeID+": "+err.Error())
	}

	if len(errorsOut) == 0 {
		return nil, fmt.Errorf("search shard %s/%s shard %d failed", target.IndexName, target.Day, target.ShardID)
	}
	return nil, fmt.Errorf("search shard %s/%s shard %d failed: %s", target.IndexName, target.Day, target.ShardID, strings.Join(errorsOut, "; "))
}

func (s *Server) fetchSearchHits(ctx context.Context, refs []searchHitRef) ([]ShardHit, []string) {
	if len(refs) == 0 {
		return nil, nil
	}

	type shardFetchGroup struct {
		target RoutingEntry
		docIDs []string
	}

	groups := make(map[string]*shardFetchGroup)
	for _, ref := range refs {
		key := routingMapKey(ref.Target.IndexName, ref.Target.Day, ref.Target.ShardID)
		group, ok := groups[key]
		if !ok {
			group = &shardFetchGroup{
				target: ref.Target,
				docIDs: make([]string, 0, len(refs)),
			}
			groups[key] = group
		}
		group.docIDs = append(group.docIDs, ref.DocID)
	}

	type result struct {
		key  string
		docs map[string]Document
		err  error
	}

	results := make(chan result, len(groups))
	sem := make(chan struct{}, effectiveSearchShardConcurrency(len(groups)))
	var wg sync.WaitGroup

	for key, group := range groups {
		key := key
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			docs, err := s.fetchDocumentsForShard(ctx, group.target, group.docIDs)
			results <- result{key: key, docs: docs, err: err}
		}()
	}

	wg.Wait()
	close(results)

	docByShardAndID := make(map[string]map[string]Document, len(groups))
	partial := make([]string, 0)
	for res := range results {
		if res.err != nil {
			partial = append(partial, res.err.Error())
			continue
		}
		docByShardAndID[res.key] = res.docs
	}

	hits := make([]ShardHit, 0, len(refs))
	for _, ref := range refs {
		key := routingMapKey(ref.Target.IndexName, ref.Target.Day, ref.Target.ShardID)
		docs := docByShardAndID[key]
		source, ok := docs[ref.DocID]
		if !ok {
			partial = append(partial, fmt.Sprintf("fetch docs %s/%s shard %d missing %s", ref.Target.IndexName, ref.Target.Day, ref.Target.ShardID, ref.DocID))
			continue
		}
		hits = append(hits, ShardHit{
			Index:  ref.Target.IndexName,
			Day:    ref.Target.Day,
			Shard:  ref.Target.ShardID,
			Score:  ref.Score,
			DocID:  ref.DocID,
			Source: source,
		})
	}
	return hits, partial
}

func (s *Server) fetchDocumentsForShard(ctx context.Context, target RoutingEntry, docIDs []string) (map[string]Document, error) {
	tried := make(map[string]struct{}, len(target.Replicas))
	errorsOut := make([]string, 0, len(target.Replicas))
	routeKey := routingMapKey(target.IndexName, target.Day, target.ShardID)

	for attempt := 0; attempt < len(target.Replicas); attempt++ {
		replicaNodeID, addr, err := s.pickReplicaForRoute(ctx, target, tried)
		if err != nil {
			if len(errorsOut) == 0 {
				return nil, err
			}
			break
		}
		tried[replicaNodeID] = struct{}{}

		var resp FetchDocsResponse
		err = s.postJSON(ctx, addr+"/internal/fetch_docs", FetchDocsRequest{
			IndexName: target.IndexName,
			Day:       target.Day,
			ShardID:   target.ShardID,
			DocIDs:    append([]string(nil), docIDs...),
		}, &resp)
		if err == nil {
			out := make(map[string]Document, len(resp.Docs))
			for _, doc := range resp.Docs {
				out[doc.DocID] = doc.Source
			}
			return out, nil
		}

		s.invalidateReplica(routeKey, replicaNodeID)
		errorsOut = append(errorsOut, replicaNodeID+": "+err.Error())
	}

	if len(errorsOut) == 0 {
		return nil, fmt.Errorf("fetch docs %s/%s shard %d failed", target.IndexName, target.Day, target.ShardID)
	}
	return nil, fmt.Errorf("fetch docs %s/%s shard %d failed: %s", target.IndexName, target.Day, target.ShardID, strings.Join(errorsOut, "; "))
}

func (s *Server) handleSearchShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SearchShardRequest
	if err := decodeJSONRequest(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idx, err := s.openExistingShardIndex(req.IndexName, req.Day, req.ShardID)
	if err != nil {
		if err == errShardIndexMissing {
			http.Error(w, "shard not available", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	searchReq := bleve.NewSearchRequestOptions(buildBleveQuery(req.Query), req.K, 0, false)
	if req.FetchDocs {
		searchReq.Fields = []string{"*"}
	}
	searchResult, err := idx.Search(searchReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := SearchShardResponse{Hits: make([]ShardHit, 0, len(searchResult.Hits))}
	for _, h := range searchResult.Hits {
		hit := ShardHit{Index: req.IndexName, Day: req.Day, Shard: req.ShardID, Score: h.Score, DocID: h.ID}
		if req.FetchDocs {
			hit.Source = docFromBleveFields(h.Fields)
		}
		resp.Hits = append(resp.Hits, hit)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleFetchDocs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FetchDocsRequest
	if err := decodeJSONRequest(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.ownsReplica(req.IndexName, req.Day, req.ShardID) && !s.localShardExists(req.IndexName, req.Day, req.ShardID) {
		http.Error(w, "replica not assigned", http.StatusForbidden)
		return
	}

	idx, err := s.openExistingShardIndex(req.IndexName, req.Day, req.ShardID)
	if err != nil {
		if err == errShardIndexMissing {
			http.Error(w, "shard not available", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	docs, err := fetchDocumentsByID(idx, req.DocIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := FetchDocsResponse{Docs: make([]FetchedDocument, 0, len(req.DocIDs))}
	for _, docID := range req.DocIDs {
		source, ok := docs[docID]
		if !ok {
			continue
		}
		resp.Docs = append(resp.Docs, FetchedDocument{
			DocID:  docID,
			Source: source,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildBleveQuery(q string) query.Query {
	q = strings.TrimSpace(q)
	if q == "" {
		return bleve.NewMatchAllQuery()
	}
	return bleve.NewQueryStringQuery(q)
}

func resolveSearchDays(r *http.Request) ([]string, error) {
	from := strings.TrimSpace(r.URL.Query().Get("day_from"))
	to := strings.TrimSpace(r.URL.Query().Get("day_to"))
	if from == "" || to == "" {
		return nil, errors.New("provide both day_from and day_to")
	}
	start, err := time.Parse("2006-01-02", from)
	if err != nil {
		return nil, errors.New("invalid day_from")
	}
	end, err := time.Parse("2006-01-02", to)
	if err != nil {
		return nil, errors.New("invalid day_to")
	}
	if end.Before(start) {
		return nil, errors.New("day_to must be >= day_from")
	}
	var days []string
	for d := start; !d.After(end); d = d.Add(24 * time.Hour) {
		days = append(days, d.Format("2006-01-02"))
	}
	return days, nil
}

func effectiveSearchShardConcurrency(targetCount int) int {
	if targetCount <= 0 {
		return 1
	}
	concurrency := defaultSearchShardConcurrency
	if cpuCount := runtime.GOMAXPROCS(0); cpuCount > 0 {
		cpuTarget := cpuCount * 4
		if cpuTarget > concurrency {
			concurrency = cpuTarget
		}
	}
	if concurrency > maxSearchShardConcurrency {
		concurrency = maxSearchShardConcurrency
	}
	if concurrency > targetCount {
		concurrency = targetCount
	}
	if concurrency < 1 {
		return 1
	}
	return concurrency
}

func compareSearchRefs(a, b searchHitRef) int {
	switch {
	case a.Score > b.Score:
		return 1
	case a.Score < b.Score:
		return -1
	case a.DocID < b.DocID:
		return 1
	case a.DocID > b.DocID:
		return -1
	default:
		return 0
	}
}

func (s *Server) tryAcquireSearchAdmission() bool {
	select {
	case s.searchAdmission <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseSearchAdmission() {
	select {
	case <-s.searchAdmission:
	default:
	}
}

func normalizeSearchIndexScope(indexName string) string {
	indexName = strings.TrimSpace(indexName)
	switch strings.ToLower(indexName) {
	case "", "*", "_all", "all":
		return ""
	default:
		return indexName
	}
}

func searchIndexLabel(indexName string) string {
	if indexName == "" {
		return "_all"
	}
	return indexName
}

func targetIndexNames(targets []RoutingEntry) []string {
	seen := make(map[string]struct{}, len(targets))
	indexes := make([]string, 0, len(targets))
	for _, target := range targets {
		if _, ok := seen[target.IndexName]; ok {
			continue
		}
		seen[target.IndexName] = struct{}{}
		indexes = append(indexes, target.IndexName)
	}
	sort.Strings(indexes)
	return indexes
}
