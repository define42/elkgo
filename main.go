package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	blevemapping "github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Distributed full-text search in Go with:
// - etcd-based membership and routing
// - fixed primary per shard (first replica in routing list)
// - background repair for failed replicas
// - enforced time-based partitioning: 48 shards per day, per index
// - generic JSON ingestion (requires id + timestamp-like field)
//
// Time partitioning rules:
// - every index/day partition always has exactly 48 shards
// - routing key is: /distsearch/routing/<index>/<YYYY-MM-DD>/<shard>
// - document day is derived from timestamp/event_time/created/ts/@timestamp
//
// Example startup:
//   go run . -mode=both -node-id=n1 -listen=:8081 -data=./data -etcd-endpoints=http://127.0.0.1:2379
//   go run . -mode=node -node-id=n2 -listen=:8082 -data=./data -etcd-endpoints=http://127.0.0.1:2379
//   go run . -mode=node -node-id=n3 -listen=:8083 -data=./data -etcd-endpoints=http://127.0.0.1:2379
//
// Bootstrap one index/day routing set:
//   curl -XPOST 'http://127.0.0.1:8081/admin/bootstrap?index=events&day=2026-03-21&replication_factor=3'
//
// Ingest generic JSON:
//   curl -XPOST 'http://127.0.0.1:8081/index?index=events' -H 'content-type: application/json' -d '{
//     "id":"evt-1",
//     "timestamp":"2026-03-21T12:34:00Z",
//     "service":"api",
//     "level":"error",
//     "message":"timeout talking to etcd",
//     "tags":["prod","search"]
//   }'
//
// Search a day:
//   curl 'http://127.0.0.1:8081/search?index=events&day=2026-03-21&q=timeout+etcd&k=10'
//
// Search a day range:
//   curl 'http://127.0.0.1:8081/search?index=events&day_from=2026-03-20&day_to=2026-03-21&q=service:api+level:error&k=10'

type Document map[string]interface{}

type NodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

const enforcedShardsPerDay = 48

type RoutingEntry struct {
	IndexName string   `json:"index_name"`
	Day       string   `json:"day"`
	ShardID   int      `json:"shard_id"`
	Replicas  []string `json:"replicas"`
	Version   int64    `json:"version"`
	UpdatedAt string   `json:"updated_at"`
}

type MemberLease struct {
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	StartedAt string `json:"started_at"`
}

type ShardHit struct {
	Index  string   `json:"index"`
	Day    string   `json:"day"`
	Shard  int      `json:"shard"`
	Score  float64  `json:"score"`
	DocID  string   `json:"doc_id"`
	Source Document `json:"source"`
}

type SearchShardRequest struct {
	IndexName string `json:"index_name"`
	Day       string `json:"day"`
	ShardID   int    `json:"shard_id"`
	Query     string `json:"query"`
	K         int    `json:"k"`
}

type SearchShardResponse struct {
	Hits []ShardHit `json:"hits"`
}

type internalIndexRequest struct {
	IndexName string   `json:"index_name"`
	Day       string   `json:"day"`
	ShardID   int      `json:"shard_id"`
	DocID     string   `json:"doc_id"`
	Doc       Document `json:"doc"`
}

type DumpDocsResponse struct {
	Docs []Document `json:"docs"`
}

type nodeServer struct {
	nodeID  string
	listen  string
	dataDir string
	mode    string

	client *http.Client

	mu      sync.RWMutex
	indexes map[string]bleve.Index

	etcd              *clientv3.Client
	etcdEndpoints     []string
	memberLeaseID     clientv3.LeaseID
	memberLeaseCancel context.CancelFunc

	routingMu sync.RWMutex
	routing   map[string]RoutingEntry

	membersMu sync.RWMutex
	members   map[string]NodeInfo

	replicationFactor int
	routingPrefix     string
	memberPrefix      string
}

func main() {
	var mode string
	var nodeID string
	var listen string
	var dataDir string
	var etcdEndpointsRaw string
	var replicationFactor int

	flag.StringVar(&mode, "mode", "both", "node|coordinator|both")
	flag.StringVar(&nodeID, "node-id", "n1", "node id")
	flag.StringVar(&listen, "listen", ":8081", "listen address")
	flag.StringVar(&dataDir, "data", "./data", "data directory")
	flag.StringVar(&etcdEndpointsRaw, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	flag.IntVar(&replicationFactor, "replication-factor", 3, "default replica count for bootstrap")
	flag.Parse()

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		log.Fatal("at least one etcd endpoint is required")
	}

	s, err := newNodeServer(mode, nodeID, listen, dataDir, endpoints, replicationFactor)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}
	defer s.close()

	ctx := context.Background()
	if err := s.connectEtcd(ctx); err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	if err := s.registerMember(ctx); err != nil {
		log.Fatalf("register member: %v", err)
	}
	if err := s.loadMembers(ctx); err != nil {
		log.Fatalf("load members: %v", err)
	}
	if err := s.loadRouting(ctx); err != nil {
		log.Fatalf("load routing: %v", err)
	}
	go s.watchMembers(context.Background())
	go s.watchRouting(context.Background())
	go s.repairLoop(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/admin/bootstrap", s.handleBootstrap)
	mux.HandleFunc("/admin/routing", s.handleRouting)
	mux.HandleFunc("/index", s.handleIndex)
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/internal/index", s.handleInternalIndex)
	mux.HandleFunc("/internal/search_shard", s.handleSearchShard)
	mux.HandleFunc("/internal/dump_docs", s.handleDumpDocs)

	log.Printf("starting mode=%s node=%s listen=%s etcd=%v", mode, nodeID, listen, endpoints)
	if err := http.ListenAndServe(listen, loggingMiddleware(mux)); err != nil {
		log.Fatal(err)
	}
}

func newNodeServer(mode, nodeID, listen, dataDir string, etcdEndpoints []string, replicationFactor int) (*nodeServer, error) {
	return &nodeServer{
		nodeID:            nodeID,
		listen:            listen,
		dataDir:           dataDir,
		mode:              mode,
		client:            &http.Client{Timeout: 8 * time.Second},
		indexes:           map[string]bleve.Index{},
		etcdEndpoints:     etcdEndpoints,
		routing:           map[string]RoutingEntry{},
		members:           map[string]NodeInfo{},
		replicationFactor: replicationFactor,
		routingPrefix:     "/distsearch/routing/",
		memberPrefix:      "/distsearch/members/",
	}, nil
}

func (s *nodeServer) close() {
	if s.memberLeaseCancel != nil {
		s.memberLeaseCancel()
	}
	s.mu.Lock()
	for _, idx := range s.indexes {
		_ = idx.Close()
	}
	s.mu.Unlock()
	if s.etcd != nil {
		_ = s.etcd.Close()
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func (s *nodeServer) connectEtcd(_ context.Context) error {
	cli, err := clientv3.New(clientv3.Config{Endpoints: s.etcdEndpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	s.etcd = cli
	return nil
}

func (s *nodeServer) registerMember(ctx context.Context) error {
	lease, err := s.etcd.Grant(ctx, 15)
	if err != nil {
		return err
	}
	s.memberLeaseID = lease.ID
	member := MemberLease{NodeID: s.nodeID, Addr: publicAddrFromListen(s.listen), StartedAt: time.Now().UTC().Format(time.RFC3339)}
	b, err := json.Marshal(member)
	if err != nil {
		return err
	}
	if _, err := s.etcd.Put(ctx, s.memberPrefix+s.nodeID, string(b), clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	keepCtx, cancel := context.WithCancel(context.Background())
	s.memberLeaseCancel = cancel
	ch, err := s.etcd.KeepAlive(keepCtx, lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for range ch {
		}
	}()
	return nil
}

func (s *nodeServer) loadMembers(ctx context.Context) error {
	resp, err := s.etcd.Get(ctx, s.memberPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	members := map[string]NodeInfo{}
	for _, kv := range resp.Kvs {
		var m MemberLease
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			continue
		}
		members[m.NodeID] = NodeInfo{ID: m.NodeID, Addr: strings.TrimRight(m.Addr, "/")}
	}
	s.membersMu.Lock()
	s.members = members
	s.membersMu.Unlock()
	return nil
}

func (s *nodeServer) watchMembers(ctx context.Context) {
	watchCh := s.etcd.Watch(ctx, s.memberPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch members error: %v", wr.Err())
			continue
		}
		_ = s.loadMembers(context.Background())
	}
}

func (s *nodeServer) loadRouting(ctx context.Context) error {
	resp, err := s.etcd.Get(ctx, s.routingPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	routing := map[string]RoutingEntry{}
	for _, kv := range resp.Kvs {
		var rt RoutingEntry
		if err := json.Unmarshal(kv.Value, &rt); err != nil {
			continue
		}
		routing[routingMapKey(rt.IndexName, rt.Day, rt.ShardID)] = rt
	}
	s.routingMu.Lock()
	s.routing = routing
	s.routingMu.Unlock()
	return nil
}

func (s *nodeServer) watchRouting(ctx context.Context) {
	watchCh := s.etcd.Watch(ctx, s.routingPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch routing error: %v", wr.Err())
			continue
		}
		_ = s.loadRouting(context.Background())
	}
}

func (s *nodeServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "node_id": s.nodeID, "members": s.snapshotMembers()})
}

func (s *nodeServer) handleBootstrap(w http.ResponseWriter, r *http.Request) {
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
	nodes := make([]NodeInfo, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, m)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer sess.Close()
	elect := concurrency.NewElection(sess, "/distsearch/admin/bootstrap")
	campaignCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := elect.Campaign(campaignCtx, s.nodeID); err != nil {
		http.Error(w, fmt.Sprintf("bootstrap leadership failed: %v", err), http.StatusConflict)
		return
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	routes := generateRouting(nodes, enforcedShardsPerDay, rf)
	var created []RoutingEntry
	for shardID, replicas := range routes {
		entry := RoutingEntry{IndexName: indexName, Day: day, ShardID: shardID, Replicas: replicas, Version: time.Now().UnixNano(), UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
		b, _ := json.Marshal(entry)
		if _, err := s.etcd.Put(r.Context(), s.routingKey(indexName, day, shardID), string(b)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		created = append(created, entry)
	}
	_ = s.loadRouting(context.Background())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": indexName, "day": day, "shards_per_day": enforcedShardsPerDay, "replication_factor": rf, "routes": created})
}

func (s *nodeServer) handleRouting(w http.ResponseWriter, r *http.Request) {
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

func (s *nodeServer) handleIndex(w http.ResponseWriter, r *http.Request) {
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

func (s *nodeServer) handlePrimaryIndex(w http.ResponseWriter, r *http.Request, indexName, day string, shardID int, route RoutingEntry, docID string, doc Document) {
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

func (s *nodeServer) handleInternalIndex(w http.ResponseWriter, r *http.Request) {
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

func (s *nodeServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	if indexName == "" {
		http.Error(w, "missing index", http.StatusBadRequest)
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		http.Error(w, "missing q", http.StatusBadRequest)
		return
	}
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
	routes := s.snapshotRouting()
	targets := collectTargets(routes, indexName, days)
	if len(targets) == 0 {
		http.Error(w, "routing not initialized for requested index/day range", http.StatusServiceUnavailable)
		return
	}
	type result struct {
		Hits []ShardHit
		Err  error
	}
	ch := make(chan result, len(targets))
	for _, target := range targets {
		go func(target RoutingEntry) {
			replicaNodeID, err := s.pickHealthyReplica(r.Context(), target.IndexName, target.Day, target.ShardID)
			if err != nil {
				ch <- result{Err: err}
				return
			}
			addr, ok := s.memberAddr(replicaNodeID)
			if !ok {
				ch <- result{Err: fmt.Errorf("replica %s has no address", replicaNodeID)}
				return
			}
			var resp SearchShardResponse
			err = s.postJSON(r.Context(), addr+"/internal/search_shard", SearchShardRequest{IndexName: target.IndexName, Day: target.Day, ShardID: target.ShardID, Query: q, K: k * 2}, &resp)
			ch <- result{Hits: resp.Hits, Err: err}
		}(target)
	}
	allHits := make([]ShardHit, 0, len(targets)*k)
	var partial []string
	for i := 0; i < len(targets); i++ {
		res := <-ch
		if res.Err != nil {
			partial = append(partial, res.Err.Error())
			continue
		}
		allHits = append(allHits, res.Hits...)
	}
	sort.Slice(allHits, func(i, j int) bool {
		if allHits[i].Score == allHits[j].Score {
			return allHits[i].DocID < allHits[j].DocID
		}
		return allHits[i].Score > allHits[j].Score
	})
	if len(allHits) > k {
		allHits = allHits[:k]
	}
	writeJSON(w, http.StatusOK, map[string]any{"index": indexName, "days": days, "query": q, "k": k, "hits": allHits, "partial_errors": partial, "shards_per_day": enforcedShardsPerDay})
}

func (s *nodeServer) handleSearchShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SearchShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idx, err := s.openShardIndex(req.IndexName, req.Day, req.ShardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	searchReq := bleve.NewSearchRequestOptions(buildBleveQuery(req.Query), req.K, 0, false)
	searchReq.Fields = []string{"*"}
	searchResult, err := idx.Search(searchReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := SearchShardResponse{Hits: make([]ShardHit, 0, len(searchResult.Hits))}
	for _, h := range searchResult.Hits {
		resp.Hits = append(resp.Hits, ShardHit{Index: req.IndexName, Day: req.Day, Shard: req.ShardID, Score: h.Score, DocID: h.ID, Source: docFromBleveFields(h.Fields)})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *nodeServer) handleDumpDocs(w http.ResponseWriter, r *http.Request) {
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

func (s *nodeServer) dumpAllDocs(idx bleve.Index) ([]Document, error) {
	req := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), 10000, 0, false)
	req.Fields = []string{"*"}
	res, err := idx.Search(req)
	if err != nil {
		return nil, err
	}
	out := make([]Document, 0, len(res.Hits))
	for _, h := range res.Hits {
		doc := docFromBleveFields(h.Fields)
		if _, ok := doc["id"]; !ok {
			doc["id"] = h.ID
		}
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"]) })
	return out, nil
}

func docFromBleveFields(fields map[string]interface{}) Document {
	doc := Document{}
	for k, v := range fields {
		doc[k] = normalizeBleveField(v)
	}
	return doc
}

func normalizeBleveField(v interface{}) interface{} {
	switch x := v.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(x))
		for _, e := range x {
			out = append(out, normalizeBleveField(e))
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[k] = normalizeBleveField(e)
		}
		return out
	default:
		return x
	}
}

func (s *nodeServer) openShardIndex(indexName, day string, shardID int) (bleve.Index, error) {
	cacheKey := partitionKey(indexName, day, shardID)
	s.mu.RLock()
	idx, ok := s.indexes[cacheKey]
	s.mu.RUnlock()
	if ok {
		return idx, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.indexes[cacheKey]; ok {
		return idx, nil
	}
	path := filepath.Join(s.dataDir, s.nodeID, indexName, day, fmt.Sprintf("shard-%02d.bleve", shardID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	var err error
	if _, statErr := os.Stat(path); statErr == nil {
		idx, err = bleve.Open(path)
	} else {
		idx, err = bleve.New(path, buildIndexMapping())
	}
	if err != nil {
		return nil, err
	}
	s.indexes[cacheKey] = idx
	return idx, nil
}

func buildIndexMapping() blevemapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultAnalyzer = "standard"
	docMapping := bleve.NewDocumentMapping()
	textField := bleve.NewTextFieldMapping()
	textField.Store = true
	textField.Index = true
	textField.IncludeInAll = true
	keywordField := bleve.NewTextFieldMapping()
	keywordField.Store = true
	keywordField.Index = true
	keywordField.Analyzer = "keyword"
	numField := bleve.NewNumericFieldMapping()
	numField.Store = true
	numField.Index = true
	dateField := bleve.NewDateTimeFieldMapping()
	dateField.Store = true
	dateField.Index = true
	docMapping.Dynamic = true
	docMapping.AddFieldMappingsAt("id", keywordField)
	docMapping.AddFieldMappingsAt("title", textField)
	docMapping.AddFieldMappingsAt("body", textField)
	docMapping.AddFieldMappingsAt("message", textField)
	docMapping.AddFieldMappingsAt("tags", textField)
	docMapping.AddFieldMappingsAt("timestamp", dateField)
	docMapping.AddFieldMappingsAt("created", dateField)
	docMapping.AddFieldMappingsAt("event_time", dateField)
	docMapping.AddFieldMappingsAt("partition_day", keywordField)
	docMapping.AddFieldMappingsAt("count", numField)
	docMapping.AddFieldMappingsAt("score", numField)
	indexMapping.DefaultMapping = docMapping
	return indexMapping
}

func buildBleveQuery(q string) query.Query {
	q = strings.TrimSpace(q)
	if q == "" {
		return bleve.NewMatchAllQuery()
	}
	return bleve.NewQueryStringQuery(q)
}

func normalizeGenericDocument(doc Document) (string, string, error) {
	if doc == nil {
		return "", "", errors.New("document is required")
	}
	id, ok := asString(doc["id"])
	if !ok || strings.TrimSpace(id) == "" {
		return "", "", errors.New("document must contain a non-empty string field: id")
	}
	doc["id"] = id
	if title, ok := doc["title"]; ok {
		doc["title"] = fmt.Sprint(title)
	}
	if body, ok := doc["body"]; ok {
		doc["body"] = fmt.Sprint(body)
	}
	if msg, ok := doc["message"]; ok {
		doc["message"] = fmt.Sprint(msg)
	}
	if tags, ok := doc["tags"]; ok {
		doc["tags"] = normalizeStringArray(tags)
	}
	day, err := extractEventDay(doc)
	if err != nil {
		return "", "", err
	}
	doc["partition_day"] = day
	return id, day, nil
}

func extractEventDay(doc Document) (string, error) {
	candidates := []string{"timestamp", "event_time", "created", "ts", "@timestamp"}
	for _, key := range candidates {
		if raw, ok := doc[key]; ok {
			parsed, err := parseTimeValue(raw)
			if err == nil {
				return parsed.UTC().Format("2006-01-02"), nil
			}
		}
	}
	return "", errors.New("document must contain a parseable timestamp field: timestamp, event_time, created, ts, or @timestamp")
}

func parseTimeValue(v interface{}) (time.Time, error) {
	s := fmt.Sprint(v)
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time value: %v", v)
}

func asString(v interface{}) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func normalizeStringArray(v interface{}) []string {
	switch x := v.(type) {
	case []string:
		return append([]string(nil), x...)
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, fmt.Sprint(e))
		}
		return out
	case string:
		return []string{x}
	default:
		return []string{fmt.Sprint(v)}
	}
}

func (s *nodeServer) routeForDoc(indexName, day, docID string) (int, RoutingEntry, error) {
	shardID := keyToShard(docID, enforcedShardsPerDay)
	rt, ok := s.getRouting(indexName, day, shardID)
	if !ok {
		return 0, RoutingEntry{}, fmt.Errorf("no routing for %s/%s shard %d", indexName, day, shardID)
	}
	return shardID, rt, nil
}

func (s *nodeServer) routingKey(indexName, day string, shardID int) string {
	return s.routingPrefix + indexName + "/" + day + "/" + strconv.Itoa(shardID)
}

func routingMapKey(indexName, day string, shardID int) string {
	return indexName + "|" + day + "|" + strconv.Itoa(shardID)
}

func partitionKey(indexName, day string, shardID int) string {
	return routingMapKey(indexName, day, shardID)
}

func (s *nodeServer) getRouting(indexName, day string, shardID int) (RoutingEntry, bool) {
	s.routingMu.RLock()
	defer s.routingMu.RUnlock()
	rt, ok := s.routing[routingMapKey(indexName, day, shardID)]
	return rt, ok
}

func (s *nodeServer) snapshotRouting() map[string]RoutingEntry {
	s.routingMu.RLock()
	defer s.routingMu.RUnlock()
	out := make(map[string]RoutingEntry, len(s.routing))
	for k, v := range s.routing {
		out[k] = v
	}
	return out
}

func (s *nodeServer) snapshotMembers() map[string]NodeInfo {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	out := make(map[string]NodeInfo, len(s.members))
	for k, v := range s.members {
		out[k] = v
	}
	return out
}

func (s *nodeServer) memberAddr(nodeID string) (string, bool) {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	m, ok := s.members[nodeID]
	return m.Addr, ok
}

func (s *nodeServer) ownsReplica(indexName, day string, shardID int) bool {
	rt, ok := s.getRouting(indexName, day, shardID)
	if !ok {
		return false
	}
	for _, r := range rt.Replicas {
		if r == s.nodeID {
			return true
		}
	}
	return false
}

func (s *nodeServer) pickHealthyReplica(ctx context.Context, indexName, day string, shardID int) (string, error) {
	rt, ok := s.getRouting(indexName, day, shardID)
	if !ok {
		return "", fmt.Errorf("no routing for %s/%s shard %d", indexName, day, shardID)
	}
	for _, nodeID := range rt.Replicas {
		addr, ok := s.memberAddr(nodeID)
		if !ok {
			continue
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/healthz", nil)
		resp, err := s.client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nodeID, nil
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return "", fmt.Errorf("no healthy replica for %s/%s shard %d", indexName, day, shardID)
}

func (s *nodeServer) repairLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.repairOnce(ctx)
		}
	}
}

func (s *nodeServer) repairOnce(ctx context.Context) {
	if !s.isCoordinatorMode() {
		return
	}
	routes := s.snapshotRouting()
	for _, rt := range routes {
		if len(rt.Replicas) == 0 || rt.Replicas[0] != s.nodeID {
			continue
		}
		leaderIdx, err := s.openShardIndex(rt.IndexName, rt.Day, rt.ShardID)
		if err != nil {
			log.Printf("repair open leader %s/%s shard %d: %v", rt.IndexName, rt.Day, rt.ShardID, err)
			continue
		}
		leaderDocs, err := s.dumpAllDocs(leaderIdx)
		if err != nil {
			log.Printf("repair dump leader %s/%s shard %d: %v", rt.IndexName, rt.Day, rt.ShardID, err)
			continue
		}
		for _, replica := range rt.Replicas[1:] {
			addr, ok := s.memberAddr(replica)
			if !ok {
				continue
			}
			if err := s.repairReplica(ctx, rt.IndexName, rt.Day, rt.ShardID, addr, leaderDocs); err != nil {
				log.Printf("repair %s/%s shard %d replica %s failed: %v", rt.IndexName, rt.Day, rt.ShardID, replica, err)
			}
		}
	}
}

func (s *nodeServer) repairReplica(ctx context.Context, indexName, day string, shardID int, replicaAddr string, leaderDocs []Document) error {
	var dump DumpDocsResponse
	if err := s.getJSON(ctx, replicaAddr+"/internal/dump_docs?index="+indexName+"&day="+day+"&shard="+strconv.Itoa(shardID), &dump); err != nil {
		return err
	}
	have := make(map[string]struct{}, len(dump.Docs))
	for _, doc := range dump.Docs {
		have[fmt.Sprint(doc["id"])] = struct{}{}
	}
	for _, doc := range leaderDocs {
		docID := fmt.Sprint(doc["id"])
		if _, ok := have[docID]; ok {
			continue
		}
		if err := s.postJSON(ctx, replicaAddr+"/internal/index", internalIndexRequest{IndexName: indexName, Day: day, ShardID: shardID, DocID: docID, Doc: doc}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *nodeServer) postJSON(ctx context.Context, url string, body any, out any) error {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *nodeServer) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func keyToShard(key string, numShards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}

func generateRouting(nodes []NodeInfo, numShards int, rf int) map[int][]string {
	out := make(map[int][]string, numShards)
	for shardID := 0; shardID < numShards; shardID++ {
		type scored struct{ id, score string }
		var scoredNodes []scored
		for _, n := range nodes {
			h := sha1.Sum([]byte(fmt.Sprintf("%d:%s", shardID, n.ID)))
			scoredNodes = append(scoredNodes, scored{id: n.ID, score: hex.EncodeToString(h[:])})
		}
		sort.Slice(scoredNodes, func(i, j int) bool { return scoredNodes[i].score > scoredNodes[j].score })
		replicas := make([]string, 0, rf)
		for i := 0; i < len(scoredNodes) && i < rf; i++ {
			replicas = append(replicas, scoredNodes[i].id)
		}
		out[shardID] = replicas
	}
	return out
}

func publicAddrFromListen(listen string) string {
	host := strings.TrimSpace(listen)
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "http://" + host
	}
	return strings.TrimRight(host, "/")
}

func (s *nodeServer) isCoordinatorMode() bool {
	return s.mode == "coordinator" || s.mode == "both"
}

func resolveSearchDays(r *http.Request) ([]string, error) {
	if day := strings.TrimSpace(r.URL.Query().Get("day")); day != "" {
		if _, err := time.Parse("2006-01-02", day); err != nil {
			return nil, errors.New("invalid day (YYYY-MM-DD)")
		}
		return []string{day}, nil
	}
	from := strings.TrimSpace(r.URL.Query().Get("day_from"))
	to := strings.TrimSpace(r.URL.Query().Get("day_to"))
	if from == "" || to == "" {
		return nil, errors.New("provide either day or both day_from and day_to")
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

func collectTargets(routes map[string]RoutingEntry, indexName string, days []string) []RoutingEntry {
	daySet := map[string]struct{}{}
	for _, d := range days {
		daySet[d] = struct{}{}
	}
	out := make([]RoutingEntry, 0)
	for _, rt := range routes {
		if rt.IndexName != indexName {
			continue
		}
		if _, ok := daySet[rt.Day]; !ok {
			continue
		}
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Day == out[j].Day {
			return out[i].ShardID < out[j].ShardID
		}
		return out[i].Day < out[j].Day
	})
	return out
}
