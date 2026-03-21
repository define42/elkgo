package server

import (
	"encoding/json"
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
		writeJSON(w, http.StatusOK, rt)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routing": s.snapshotRouting(), "members": s.snapshotMembers(), "shards_per_day": enforcedShardsPerDay})
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
	docID, day, err := normalizeGenericDocument(doc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	shardID, route, err := s.routeForDoc(indexName, day, docID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if len(route.Replicas) == 0 {
		http.Error(w, "no replicas for shard", http.StatusServiceUnavailable)
		return
	}
	primary := route.Replicas[0]
	if primary == s.nodeID {
		s.handlePrimaryIndex(w, r, indexName, day, shardID, route, docID, doc)
		return
	}
	primaryAddr, ok := s.memberAddr(primary)
	if !ok {
		http.Error(w, "primary not registered", http.StatusServiceUnavailable)
		return
	}
	var out map[string]any
	if err := s.postJSON(r.Context(), primaryAddr+"/internal/index", internalIndexRequest{IndexName: indexName, Day: day, ShardID: shardID, DocID: docID, Doc: doc}, &out); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePrimaryIndex(w http.ResponseWriter, r *http.Request, indexName, day string, shardID int, route RoutingEntry, docID string, doc Document) {
	idx, err := s.openShardIndex(indexName, day, shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := idx.Index(docID, doc); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	acks := 1
	var errs []string
	for _, replica := range route.Replicas[1:] {
		addr, ok := s.memberAddr(replica)
		if !ok {
			errs = append(errs, replica+": not registered")
			continue
		}
		if err := s.postJSON(r.Context(), addr+"/internal/index", internalIndexRequest{IndexName: indexName, Day: day, ShardID: shardID, DocID: docID, Doc: doc}, nil); err != nil {
			errs = append(errs, replica+": "+err.Error())
			continue
		}
		acks++
	}
	quorum := len(route.Replicas)/2 + 1
	if acks < quorum {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "index": indexName, "day": day, "shard": shardID, "primary": s.nodeID, "replicas": route.Replicas, "acks": acks, "quorum": quorum, "errors": errs})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": indexName, "day": day, "shard": shardID, "primary": s.nodeID, "replicas": route.Replicas, "acks": acks, "errors": errs})
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
	idx, err := s.openShardIndex(req.IndexName, req.Day, req.ShardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := idx.Index(docID, req.Doc); err != nil {
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
