package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

const (
	maxBulkErrors         = 20
	bulkIngestConcurrency = 8
)

type bulkPreparedItem struct {
	lineNo    int
	indexName string
	day       string
	shardID   int
	route     RoutingEntry
	item      internalIndexBatchItem
}

type bulkShardGroup struct {
	indexName string
	day       string
	shardID   int
	route     RoutingEntry
	items     []bulkPreparedItem
}

type bulkGroupResult struct {
	group bulkShardGroup
	err   error
}

func (s *Server) handleBulkIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	if indexName == "" {
		http.Error(w, "missing index", http.StatusBadRequest)
		return
	}

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	lineNo := 0
	indexed := 0
	failed := 0
	errorsOut := make([]string, 0, maxBulkErrors)
	prepared := make([]bulkPreparedItem, 0, 128)

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var doc Document
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			failed++
			appendBulkError(&errorsOut, fmt.Sprintf("line %d: %v", lineNo, err))
			continue
		}

		docID, day, err := normalizeGenericDocument(doc)
		if err != nil {
			failed++
			appendBulkError(&errorsOut, fmt.Sprintf("line %d: %v", lineNo, err))
			continue
		}
		shardID, route, err := s.routeForDoc(indexName, day, docID)
		if err != nil {
			failed++
			appendBulkError(&errorsOut, fmt.Sprintf("line %d: %v", lineNo, err))
			continue
		}
		if len(route.Replicas) == 0 {
			failed++
			appendBulkError(&errorsOut, fmt.Sprintf("line %d: no replicas for shard", lineNo))
			continue
		}

		prepared = append(prepared, bulkPreparedItem{
			lineNo:    lineNo,
			indexName: indexName,
			day:       day,
			shardID:   shardID,
			route:     route,
			item: internalIndexBatchItem{
				DocID: docID,
				Doc:   doc,
			},
		})
	}

	if err := scanner.Err(); err != nil {
		failed++
		appendBulkError(&errorsOut, fmt.Sprintf("scan error: %v", err))
	}
	bulkIndexed, bulkFailed, bulkErrors := s.ingestPreparedBulk(r.Context(), prepared)
	indexed += bulkIndexed
	failed += bulkFailed
	for _, message := range bulkErrors {
		appendBulkError(&errorsOut, message)
	}

	status := http.StatusOK
	if failed > 0 && indexed > 0 {
		status = http.StatusMultiStatus
	} else if failed > 0 {
		status = http.StatusBadRequest
	}

	writeJSON(w, status, map[string]any{
		"ok":      failed == 0,
		"index":   indexName,
		"lines":   lineNo,
		"indexed": indexed,
		"failed":  failed,
		"errors":  errorsOut,
	})
}

func (s *Server) ingestPreparedBulk(ctx context.Context, prepared []bulkPreparedItem) (int, int, []string) {
	groups := groupBulkPreparedItems(prepared)
	if len(groups) == 0 {
		return 0, 0, nil
	}

	results := make(chan bulkGroupResult, len(groups))
	sem := make(chan struct{}, bulkIngestConcurrency)
	var wg sync.WaitGroup

	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results <- bulkGroupResult{
				group: group,
				err:   s.ingestBulkShardGroup(ctx, group),
			}
		}()
	}

	wg.Wait()
	close(results)

	indexed := 0
	failed := 0
	errorsOut := make([]string, 0, len(groups))
	for result := range results {
		if result.err != nil {
			failed += len(result.group.items)
			errorsOut = append(errorsOut, formatBulkGroupError(result.group, result.err))
			continue
		}
		indexed += len(result.group.items)
	}

	return indexed, failed, errorsOut
}

func (s *Server) ingestBulkShardGroup(ctx context.Context, group bulkShardGroup) error {
	req := internalIndexBatchRequest{
		IndexName: group.indexName,
		Day:       group.day,
		ShardID:   group.shardID,
		Replicate: true,
		Items:     make([]internalIndexBatchItem, len(group.items)),
	}
	for i, item := range group.items {
		req.Items[i] = item.item
	}

	primary := group.route.Replicas[0]
	if primary == s.nodeID {
		status, resp, err := s.indexBatchOnPrimary(ctx, group.indexName, group.day, group.shardID, group.route, req.Items)
		if err != nil {
			return err
		}
		if status/100 != 2 {
			if len(resp.Errors) > 0 {
				return fmt.Errorf("quorum not reached (acks=%d quorum=%d): %s", resp.Acks, resp.Quorum, strings.Join(resp.Errors, "; "))
			}
			return fmt.Errorf("quorum not reached (acks=%d quorum=%d)", resp.Acks, resp.Quorum)
		}
		return nil
	}

	primaryAddr, ok := s.memberAddr(primary)
	if !ok {
		return errors.New("primary not registered")
	}
	var resp internalIndexBatchResponse
	if err := s.postJSON(ctx, primaryAddr+"/internal/index_batch", req, &resp); err != nil {
		return err
	}
	if !resp.OK {
		if len(resp.Errors) > 0 {
			return fmt.Errorf("primary batch failed: %s", strings.Join(resp.Errors, "; "))
		}
		return fmt.Errorf("primary batch failed")
	}
	return nil
}

func (s *Server) ingestDocument(ctx context.Context, indexName string, doc Document) (int, map[string]any, error) {
	docID, day, err := normalizeGenericDocument(doc)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	shardID, route, err := s.routeForDoc(indexName, day, docID)
	if err != nil {
		return http.StatusServiceUnavailable, nil, err
	}
	if len(route.Replicas) == 0 {
		return http.StatusServiceUnavailable, nil, errors.New("no replicas for shard")
	}
	primary := route.Replicas[0]
	if primary == s.nodeID {
		return s.indexOnPrimary(ctx, indexName, day, shardID, route, docID, doc)
	}
	primaryAddr, ok := s.memberAddr(primary)
	if !ok {
		return http.StatusServiceUnavailable, nil, errors.New("primary not registered")
	}
	var out map[string]any
	if err := s.postJSON(ctx, primaryAddr+"/internal/index", internalIndexRequest{
		IndexName: indexName,
		Day:       day,
		ShardID:   shardID,
		DocID:     docID,
		Doc:       doc,
	}, &out); err != nil {
		return http.StatusServiceUnavailable, nil, err
	}
	return http.StatusOK, out, nil
}

func (s *Server) handleInternalIndexBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req internalIndexBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Items) == 0 {
		writeJSON(w, http.StatusOK, internalIndexBatchResponse{
			OK:      true,
			Index:   req.IndexName,
			Day:     req.Day,
			Shard:   req.ShardID,
			Indexed: 0,
		})
		return
	}

	route, ok := s.getRouting(req.IndexName, req.Day, req.ShardID)
	if !ok {
		http.Error(w, "routing not initialized for shard", http.StatusServiceUnavailable)
		return
	}

	if req.Replicate {
		if len(route.Replicas) == 0 || route.Replicas[0] != s.nodeID {
			http.Error(w, "primary not assigned to this node", http.StatusForbidden)
			return
		}
		status, resp, err := s.indexBatchOnPrimary(r.Context(), req.IndexName, req.Day, req.ShardID, route, req.Items)
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}
		writeJSON(w, status, resp)
		return
	}

	if !s.ownsReplica(req.IndexName, req.Day, req.ShardID) {
		http.Error(w, "replica not assigned to this node", http.StatusForbidden)
		return
	}
	if err := s.indexBatchLocal(req.IndexName, req.Day, req.ShardID, req.Items); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, internalIndexBatchResponse{
		OK:      true,
		Index:   req.IndexName,
		Day:     req.Day,
		Shard:   req.ShardID,
		Indexed: len(req.Items),
	})
}

func (s *Server) indexOnPrimary(ctx context.Context, indexName, day string, shardID int, route RoutingEntry, docID string, doc Document) (int, map[string]any, error) {
	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	if err := idx.Index(docID, doc); err != nil {
		return http.StatusInternalServerError, nil, err
	}
	acks := 1
	var errs []string
	for _, replica := range route.Replicas[1:] {
		addr, ok := s.memberAddr(replica)
		if !ok {
			errs = append(errs, replica+": not registered")
			continue
		}
		if err := s.postJSON(ctx, addr+"/internal/index", internalIndexRequest{
			IndexName: indexName,
			Day:       day,
			ShardID:   shardID,
			DocID:     docID,
			Doc:       doc,
		}, nil); err != nil {
			errs = append(errs, replica+": "+err.Error())
			continue
		}
		acks++
	}

	quorum := len(route.Replicas)/2 + 1
	out := map[string]any{
		"ok":       acks >= quorum,
		"index":    indexName,
		"day":      day,
		"shard":    shardID,
		"primary":  s.nodeID,
		"replicas": route.Replicas,
		"acks":     acks,
		"errors":   errs,
	}

	if acks < quorum {
		out["quorum"] = quorum
		return http.StatusServiceUnavailable, out, nil
	}
	return http.StatusOK, out, nil
}

func (s *Server) indexBatchOnPrimary(ctx context.Context, indexName, day string, shardID int, route RoutingEntry, items []internalIndexBatchItem) (int, internalIndexBatchResponse, error) {
	resp := internalIndexBatchResponse{
		Index:    indexName,
		Day:      day,
		Shard:    shardID,
		Primary:  s.nodeID,
		Replicas: append([]string(nil), route.Replicas...),
		Indexed:  len(items),
	}
	if len(items) == 0 {
		resp.OK = true
		resp.Acks = 1
		resp.Quorum = 1
		return http.StatusOK, resp, nil
	}

	if err := s.indexBatchLocal(indexName, day, shardID, items); err != nil {
		return http.StatusInternalServerError, resp, err
	}

	acks := 1
	errs := make([]string, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, replica := range route.Replicas[1:] {
		replica := replica
		wg.Add(1)
		go func() {
			defer wg.Done()

			addr, ok := s.memberAddr(replica)
			if !ok {
				mu.Lock()
				errs = append(errs, replica+": not registered")
				mu.Unlock()
				return
			}

			var replicaResp internalIndexBatchResponse
			err := s.postJSON(ctx, addr+"/internal/index_batch", internalIndexBatchRequest{
				IndexName: indexName,
				Day:       day,
				ShardID:   shardID,
				Items:     items,
				Replicate: false,
			}, &replicaResp)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, replica+": "+err.Error())
				return
			}
			acks++
		}()
	}

	wg.Wait()

	resp.Acks = acks
	resp.Quorum = len(route.Replicas)/2 + 1
	resp.Errors = errs
	resp.OK = acks >= resp.Quorum
	if !resp.OK {
		return http.StatusServiceUnavailable, resp, nil
	}
	return http.StatusOK, resp, nil
}

func (s *Server) indexBatchLocal(indexName, day string, shardID int, items []internalIndexBatchItem) error {
	if len(items) == 0 {
		return nil
	}

	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		return err
	}

	batch := idx.NewBatch()
	for _, item := range items {
		docID := strings.TrimSpace(item.DocID)
		if docID == "" {
			normalizedDocID, normalizedDay, err := normalizeGenericDocument(item.Doc)
			if err != nil {
				return err
			}
			if normalizedDay != day {
				return fmt.Errorf("document day %s does not match shard day %s", normalizedDay, day)
			}
			docID = normalizedDocID
		}
		if err := batch.Index(docID, item.Doc); err != nil {
			return err
		}
	}

	return idx.Batch(batch)
}

func appendBulkError(errorsOut *[]string, message string) {
	if len(*errorsOut) >= maxBulkErrors {
		return
	}
	*errorsOut = append(*errorsOut, message)
}

func groupBulkPreparedItems(prepared []bulkPreparedItem) []bulkShardGroup {
	groups := make(map[string]*bulkShardGroup, len(prepared))
	for _, item := range prepared {
		key := routingMapKey(item.indexName, item.day, item.shardID)
		group, ok := groups[key]
		if !ok {
			group = &bulkShardGroup{
				indexName: item.indexName,
				day:       item.day,
				shardID:   item.shardID,
				route:     item.route,
			}
			groups[key] = group
		}
		group.items = append(group.items, item)
	}

	out := make([]bulkShardGroup, 0, len(groups))
	for _, group := range groups {
		out = append(out, *group)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].day == out[j].day {
			return out[i].shardID < out[j].shardID
		}
		return out[i].day < out[j].day
	})
	return out
}

func formatBulkGroupError(group bulkShardGroup, err error) string {
	start, end := group.lineRange()
	lineLabel := fmt.Sprintf("line %d", start)
	if end > start {
		lineLabel = fmt.Sprintf("lines %d-%d", start, end)
	}
	return fmt.Sprintf("%s: %s/%s shard %d: %v", lineLabel, group.indexName, group.day, group.shardID, err)
}

func (g bulkShardGroup) lineRange() (int, int) {
	if len(g.items) == 0 {
		return 0, 0
	}
	start := g.items[0].lineNo
	end := g.items[0].lineNo
	for _, item := range g.items[1:] {
		if item.lineNo < start {
			start = item.lineNo
		}
		if item.lineNo > end {
			end = item.lineNo
		}
	}
	return start, end
}
