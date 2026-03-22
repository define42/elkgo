package server

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/blevesearch/bleve/v2"
)

func New(cfg Config) *Server {
	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())

	return &Server{
		nodeID:                 cfg.NodeID,
		listen:                 cfg.Listen,
		publicAddr:             cfg.PublicAddr,
		dataDir:                cfg.DataDir,
		mode:                   cfg.Mode,
		backgroundCtx:          backgroundCtx,
		backgroundCancel:       backgroundCancel,
		client:                 &http.Client{Timeout: 8 * time.Second},
		indexes:                map[string]bleve.Index{},
		replicaCache:           map[string]string{},
		shardSyncingVersion:    map[string]int64{},
		shardSyncedVersion:     map[string]int64{},
		etcdEndpoints:          append([]string(nil), cfg.ETCDEndpoints...),
		routing:                map[string]RoutingEntry{},
		members:                map[string]NodeInfo{},
		drainStates:            map[string]NodeDrainState{},
		offlineStates:          map[string]NodeOfflineState{},
		indexRetentionPolicies: map[string]IndexRetentionPolicy{},
		replicaRepairStates:    map[string]ReplicaRepairState{},
		replicaRepairRunning:   map[string]bool{},
		replicaRepairRequests:  map[string]int64{},
		replicationFactor:      cfg.ReplicationFactor,
		routingPrefix:          "/distsearch/routing/",
		memberPrefix:           "/distsearch/members/",
		drainPrefix:            "/distsearch/drain/",
		offlinePrefix:          "/distsearch/offline/",
		indexRetentionPrefix:   "/distsearch/index-retention/",
		replicaRepairPrefix:    "/distsearch/replica-repair/",
	}
}

func (s *Server) Run() error {
	ctx := context.Background()
	if err := s.connectEtcd(ctx); err != nil {
		return err
	}
	if err := s.registerMember(ctx); err != nil {
		return err
	}
	if err := s.loadMembers(ctx); err != nil {
		return err
	}
	if err := s.loadOfflineStates(ctx); err != nil {
		return err
	}
	if err := s.loadRouting(ctx); err != nil {
		return err
	}
	if err := s.loadIndexRetentionPolicies(ctx); err != nil {
		return err
	}
	if err := s.loadReplicaRepairStates(ctx); err != nil {
		return err
	}
	if err := s.ensureOfflineMarkersForMissingRouteReplicas(ctx); err != nil {
		return err
	}
	if err := s.loadOfflineStates(ctx); err != nil {
		return err
	}
	if err := s.cleanupObsoleteReplicaRepairStates(); err != nil {
		return err
	}
	s.resumeReplicaRepairLoops()
	if err := s.runRetentionCleanup(ctx, time.Now().UTC()); err != nil {
		return err
	}

	go s.watchMembers(context.Background())
	go s.watchDrainStates(context.Background())
	go s.watchOfflineStates(context.Background())
	go s.watchRouting(context.Background())
	go s.watchIndexRetentionPolicies(context.Background())
	go s.watchReplicaRepairStates(context.Background())
	go s.offlineDrainLoop(context.Background())
	go s.retentionCleanupLoop(context.Background())

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	log.Printf("starting mode=%s node=%s listen=%s etcd=%v", s.mode, s.nodeID, s.listen, s.etcdEndpoints)
	return http.ListenAndServe(s.listen, loggingMiddleware(mux))
}

func (s *Server) Close() {
	if s.backgroundCancel != nil {
		s.backgroundCancel()
	}
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

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleHome)
	mux.HandleFunc("/cluster", s.handleCluster)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/admin/bootstrap", s.handleBootstrap)
	mux.HandleFunc("/admin/indexes", s.handleAvailableIndexes)
	mux.HandleFunc("/admin/index_retention", s.handleIndexRetention)
	mux.HandleFunc("/admin/nodes/drain", s.handleNodeDrain)
	mux.HandleFunc("/admin/routing", s.handleRouting)
	mux.HandleFunc("/index", s.handleIndex)
	mux.HandleFunc("/bulk", s.handleBulkIngest)
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/internal/index", s.handleInternalIndex)
	mux.HandleFunc("/internal/index_batch", s.handleInternalIndexBatch)
	mux.HandleFunc("/internal/search_shard", s.handleSearchShard)
	mux.HandleFunc("/internal/dump_docs", s.handleDumpDocs)
	mux.HandleFunc("/internal/stream_docs", s.handleStreamDocs)
	mux.HandleFunc("/internal/shard_stats", s.handleShardStats)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
