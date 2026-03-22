package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxBulkErrors         = 20
	bulkIngestConcurrency = 8
	routeRetryAttempts    = 5
	routeRetryDelay       = 100 * time.Millisecond
	scannerBufInitial     = 64 * 1024
	scannerBufMax         = 8 * 1024 * 1024
)

var scannerBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, scannerBufInitial)
		return &buf
	},
}

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

	bodyReader, err := requestBodyReader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer bodyReader.Close()

	bufPtr := scannerBufPool.Get().(*[]byte)
	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(*bufPtr, scannerBufMax)
	defer scannerBufPool.Put(bufPtr)

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
				Raw:   append([]byte(nil), []byte(line)...),
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
	var lastErr error
	for attempt := 0; attempt < routeRetryAttempts; attempt++ {
		route, err := s.currentRouteForShard(group.indexName, group.day, group.shardID, group.route)
		if err != nil {
			lastErr = err
		} else {
			lastErr = s.ingestBulkShardGroupOnce(ctx, group, route)
			if lastErr == nil {
				return nil
			}
		}

		if !shouldRetryRouteError(lastErr) || attempt == routeRetryAttempts-1 {
			return lastErr
		}
		if !sleepWithContext(ctx, routeRetryDelay) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return lastErr
		}
	}
	return lastErr
}

func (s *Server) ingestBulkShardGroupOnce(ctx context.Context, group bulkShardGroup, route RoutingEntry) error {
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

	primary := route.Replicas[0]
	if primary == s.nodeID {
		status, resp, err := s.indexBatchOnPrimary(ctx, group.indexName, group.day, group.shardID, route, req.Items)
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

	status, resp, err := s.indexSingleDocument(ctx, indexName, day, shardID, route, docID, doc)
	if err != nil {
		return status, nil, err
	}
	return status, singleDocumentResponse(docID, resp), nil
}

func (s *Server) handleInternalIndexBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req internalIndexBatchRequest
	if err := decodeJSONRequest(r, &req); err != nil {
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
	if !req.Repair && !s.shardReadyForReplicaTraffic(req.IndexName, req.Day, req.ShardID) {
		http.Error(w, "replica syncing", http.StatusServiceUnavailable)
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

func (s *Server) indexSingleDocument(ctx context.Context, indexName, day string, shardID int, route RoutingEntry, docID string, doc Document) (int, internalIndexBatchResponse, error) {
	items := []internalIndexBatchItem{{
		DocID: docID,
		Doc:   doc,
	}}

	lastStatus := http.StatusServiceUnavailable
	var lastResp internalIndexBatchResponse
	var lastErr error

	for attempt := 0; attempt < routeRetryAttempts; attempt++ {
		currentRoute, err := s.currentRouteForShard(indexName, day, shardID, route)
		if err != nil {
			lastErr = err
			lastStatus = http.StatusServiceUnavailable
		} else {
			primary := currentRoute.Replicas[0]
			if primary == s.nodeID {
				return s.indexBatchOnPrimary(ctx, indexName, day, shardID, currentRoute, items)
			}

			primaryAddr, ok := s.memberAddr(primary)
			if !ok {
				lastErr = errors.New("primary not registered")
				lastStatus = http.StatusServiceUnavailable
			} else {
				lastResp = internalIndexBatchResponse{}
				lastStatus, lastErr = s.postJSONStatus(ctx, primaryAddr+"/internal/index_batch", internalIndexBatchRequest{
					IndexName: indexName,
					Day:       day,
					ShardID:   shardID,
					Items:     items,
					Replicate: true,
				}, &lastResp)
				if lastErr == nil {
					return lastStatus, lastResp, nil
				}
				if lastStatus == 0 {
					lastStatus = http.StatusServiceUnavailable
				}
			}
		}

		if !shouldRetryRouteError(lastErr) || attempt == routeRetryAttempts-1 {
			break
		}
		if !sleepWithContext(ctx, routeRetryDelay) {
			if err := ctx.Err(); err != nil {
				return http.StatusServiceUnavailable, lastResp, err
			}
			break
		}
	}

	return lastStatus, lastResp, lastErr
}

func singleDocumentResponse(docID string, resp internalIndexBatchResponse) map[string]any {
	out := map[string]any{
		"ok":     resp.OK,
		"index":  resp.Index,
		"day":    resp.Day,
		"shard":  resp.Shard,
		"doc_id": docID,
		"errors": append([]string(nil), resp.Errors...),
	}
	if resp.Primary != "" {
		out["primary"] = resp.Primary
	}
	if len(resp.Replicas) > 0 {
		out["replicas"] = append([]string(nil), resp.Replicas...)
	}
	if resp.Acks > 0 {
		out["acks"] = resp.Acks
	}
	if resp.Quorum > 0 {
		out["quorum"] = resp.Quorum
	}
	return out
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
	failedReplicas := make([]string, 0)
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
				failedReplicas = append(failedReplicas, replica)
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
				failedReplicas = append(failedReplicas, replica)
				return
			}
			acks++
		}()
	}

	wg.Wait()

	for _, replica := range failedReplicas {
		s.scheduleReplicaRepair(route, replica)
	}

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

	lock := s.shardWriteLock(indexName, day, shardID)
	lock.Lock()
	defer lock.Unlock()

	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		return err
	}

	sourcePath := s.shardSourceSegmentPath(indexName, day, shardID, currentSourceSegment)
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		return err
	}
	sourceFile, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	offset, err := sourceFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	bufferedSource := bufio.NewWriter(sourceFile)

	batch := idx.NewBatch()
	for _, item := range items {
		docID, doc, raw, err := materializeBatchItemSource(item, day)
		if err != nil {
			return err
		}
		compressed := compressSourceRecord(raw)
		pointer := sourcePointer{
			Segment:          currentSourceSegment,
			Offset:           offset,
			CompressedLength: uint64(len(compressed)),
			RawLength:        uint64(len(raw)),
		}
		if err := writeSourceRecord(bufferedSource, raw, compressed); err != nil {
			return err
		}
		offset += sourceRecordHeaderSize + int64(len(compressed))
		addSourcePointerFields(doc, pointer)
		if err := batch.Index(docID, doc); err != nil {
			return err
		}
	}

	if err := bufferedSource.Flush(); err != nil {
		return err
	}
	if err := sourceFile.Sync(); err != nil {
		return err
	}
	return idx.Batch(batch)
}

func materializeBatchItem(item internalIndexBatchItem, day string) (string, Document, error) {
	var doc Document
	switch {
	case len(item.Raw) > 0:
		if err := json.Unmarshal(item.Raw, &doc); err != nil {
			return "", nil, err
		}
	case item.Doc != nil:
		doc = cloneDocument(item.Doc)
	default:
		return "", nil, errors.New("document is required")
	}

	normalizedDocID, normalizedDay, err := normalizeGenericDocument(doc)
	if err != nil {
		return "", nil, err
	}
	if normalizedDay != day {
		return "", nil, fmt.Errorf("document day %s does not match shard day %s", normalizedDay, day)
	}
	docID := strings.TrimSpace(item.DocID)
	if docID == "" {
		docID = normalizedDocID
	}
	return docID, doc, nil
}

func (s *Server) currentRouteForShard(indexName, day string, shardID int, fallback RoutingEntry) (RoutingEntry, error) {
	if route, ok := s.getRouting(indexName, day, shardID); ok && len(route.Replicas) > 0 {
		return route, nil
	}
	if len(fallback.Replicas) > 0 {
		return fallback, nil
	}
	return RoutingEntry{}, errors.New("routing not initialized for shard")
}

func shouldRetryRouteError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "primary not assigned") ||
		strings.Contains(message, "primary not registered") ||
		strings.Contains(message, "routing not initialized for shard")
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
