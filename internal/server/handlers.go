package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"elkgo/internal/webui"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "node_id": s.nodeID, "members": s.snapshotMembers()})
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, webui.HomePageHTML)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/cluster" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, webui.ClusterPageHTML)
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isCoordinatorMode() {
		http.Error(w, "bootstrap requires coordinator or both mode", http.StatusForbidden)
		return
	}
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	if indexName == "" {
		http.Error(w, "missing index", http.StatusBadRequest)
		return
	}
	day := strings.TrimSpace(r.URL.Query().Get("day"))
	if _, err := time.Parse("2006-01-02", day); err != nil {
		http.Error(w, "missing or invalid day (YYYY-MM-DD)", http.StatusBadRequest)
		return
	}
	rf := s.replicationFactor
	if raw := r.URL.Query().Get("replication_factor"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			rf = v
		}
	}
	members := s.snapshotMembers()
	if len(members) == 0 {
		http.Error(w, "no members registered", http.StatusBadRequest)
		return
	}
	if rf > len(members) {
		rf = len(members)
	}
	created, err := s.bootstrapRouting(r.Context(), indexName, day, rf)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "leadership") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": indexName, "day": day, "shards_per_day": enforcedShardsPerDay, "replication_factor": rf, "routes": created})
}

func (s *Server) handleAvailableIndexes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	byIndex := map[string]map[string]struct{}{}
	for _, route := range s.snapshotRouting() {
		if _, ok := byIndex[route.IndexName]; !ok {
			byIndex[route.IndexName] = map[string]struct{}{}
		}
		byIndex[route.IndexName][route.Day] = struct{}{}
	}

	type indexInfo struct {
		Name string   `json:"name"`
		Days []string `json:"days"`
	}

	indexes := make([]indexInfo, 0, len(byIndex))
	for name, daySet := range byIndex {
		days := make([]string, 0, len(daySet))
		for day := range daySet {
			days = append(days, day)
		}
		sort.Strings(days)
		indexes = append(indexes, indexInfo{Name: name, Days: days})
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })

	writeJSON(w, http.StatusOK, map[string]any{"indexes": indexes})
}

func (s *Server) handleRouting(w http.ResponseWriter, r *http.Request) {
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	day := strings.TrimSpace(r.URL.Query().Get("day"))
	includeStats := queryEnabled(r, "stats") || queryEnabled(r, "include_counts")
	if raw := r.URL.Query().Get("shard"); raw != "" {
		shardID, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rt, ok := s.getRouting(indexName, day, shardID)
		if !ok {
			http.Error(w, "unknown shard", http.StatusNotFound)
			return
		}
		if includeStats {
			writeJSON(w, http.StatusOK, s.routingEntryStats(r.Context(), rt))
			return
		}
		writeJSON(w, http.StatusOK, rt)
		return
	}
	routes := s.snapshotRouting()
	if includeStats {
		writeJSON(w, http.StatusOK, map[string]any{"routing": s.routingStatsMap(r.Context(), routes), "members": s.snapshotMembers(), "shards_per_day": enforcedShardsPerDay})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routing": routes, "members": s.snapshotMembers(), "shards_per_day": enforcedShardsPerDay})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	if indexName == "" {
		http.Error(w, "missing index", http.StatusBadRequest)
		return
	}
	var doc Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status, out, err := s.ingestDocument(r.Context(), indexName, doc)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, status, out)
}

func (s *Server) handleInternalIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req internalIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.ownsReplica(req.IndexName, req.Day, req.ShardID) {
		http.Error(w, "replica not assigned to this node", http.StatusForbidden)
		return
	}
	docID := req.DocID
	if docID == "" {
		var err error
		docID, _, err = normalizeGenericDocument(req.Doc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if err := s.indexBatchLocal(req.IndexName, req.Day, req.ShardID, []internalIndexBatchItem{{
		DocID: docID,
		Doc:   req.Doc,
	}}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": req.IndexName, "day": req.Day, "shard": req.ShardID, "node_id": s.nodeID, "doc_id": docID})
}

func (s *Server) handleDumpDocs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	day := strings.TrimSpace(r.URL.Query().Get("day"))
	shardID, err := strconv.Atoi(r.URL.Query().Get("shard"))
	if err != nil {
		http.Error(w, "missing or invalid shard", http.StatusBadRequest)
		return
	}
	if !s.ownsReplica(indexName, day, shardID) {
		http.Error(w, "replica not assigned", http.StatusForbidden)
		return
	}
	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	docs, err := s.dumpAllDocs(idx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, DumpDocsResponse{Docs: docs})
}

func (s *Server) handleShardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	day := strings.TrimSpace(r.URL.Query().Get("day"))
	shardID, err := strconv.Atoi(r.URL.Query().Get("shard"))
	if err != nil {
		http.Error(w, "missing or invalid shard", http.StatusBadRequest)
		return
	}
	if !s.ownsReplica(indexName, day, shardID) {
		http.Error(w, "replica not assigned", http.StatusForbidden)
		return
	}
	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	count, err := shardEventCount(idx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, ShardStatsResponse{
		IndexName:  indexName,
		Day:        day,
		ShardID:    shardID,
		EventCount: count,
	})
}

func (s *Server) routingStatsMap(ctx context.Context, routes map[string]RoutingEntry) map[string]RoutingEntryStats {
	out := make(map[string]RoutingEntryStats, len(routes))
	for key, route := range routes {
		out[key] = s.routingEntryStats(ctx, route)
	}
	return out
}

func (s *Server) routingEntryStats(ctx context.Context, route RoutingEntry) RoutingEntryStats {
	out := RoutingEntryStats{RoutingEntry: route}
	resp, err := s.fetchShardStats(ctx, route)
	if err != nil {
		out.CountError = err.Error()
		return out
	}
	out.EventCount = resp.EventCount
	return out
}

func (s *Server) fetchShardStats(ctx context.Context, route RoutingEntry) (ShardStatsResponse, error) {
	tried := make(map[string]struct{}, len(route.Replicas))
	errorsOut := make([]string, 0, len(route.Replicas))
	routeKey := routingMapKey(route.IndexName, route.Day, route.ShardID)

	for attempt := 0; attempt < len(route.Replicas); attempt++ {
		replicaNodeID, addr, err := s.pickReplicaForRoute(ctx, route, tried)
		if err != nil {
			if len(errorsOut) == 0 {
				return ShardStatsResponse{}, err
			}
			break
		}
		tried[replicaNodeID] = struct{}{}

		var resp ShardStatsResponse
		err = s.getJSON(ctx, addr+"/internal/shard_stats?index="+route.IndexName+"&day="+route.Day+"&shard="+strconv.Itoa(route.ShardID), &resp)
		if err == nil {
			return resp, nil
		}

		s.invalidateReplica(routeKey, replicaNodeID)
		errorsOut = append(errorsOut, replicaNodeID+": "+err.Error())
	}

	if len(errorsOut) == 0 {
		return ShardStatsResponse{}, fmt.Errorf("stats %s/%s shard %d failed", route.IndexName, route.Day, route.ShardID)
	}
	return ShardStatsResponse{}, fmt.Errorf("stats %s/%s shard %d failed: %s", route.IndexName, route.Day, route.ShardID, strings.Join(errorsOut, "; "))
}

func queryEnabled(r *http.Request, key string) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
