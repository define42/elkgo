package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const maxBulkErrors = 20

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

		status, out, err := s.ingestDocument(r.Context(), indexName, doc)
		if err != nil || status/100 != 2 {
			failed++
			msg := fmt.Sprintf("line %d: status %d", lineNo, status)
			if err != nil {
				msg = fmt.Sprintf("line %d: %v", lineNo, err)
			} else if out != nil {
				if detail, ok := out["errors"]; ok {
					msg = fmt.Sprintf("line %d: status %d: %v", lineNo, status, detail)
				}
			}
			appendBulkError(&errorsOut, msg)
			continue
		}

		indexed++
	}

	if err := scanner.Err(); err != nil {
		failed++
		appendBulkError(&errorsOut, fmt.Sprintf("scan error: %v", err))
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

func appendBulkError(errorsOut *[]string, message string) {
	if len(*errorsOut) >= maxBulkErrors {
		return
	}
	*errorsOut = append(*errorsOut, message)
}
