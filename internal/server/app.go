package server

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/blevesearch/bleve/v2"
)

const (
	serverReadHeaderTimeout    = 5 * time.Second
	serverReadTimeout          = 2 * time.Minute
	serverWriteTimeout         = 2 * time.Minute
	serverIdleTimeout          = 2 * time.Minute
	serverShutdownTimeout      = 10 * time.Second
	defaultSearchRequestLimit  = 32
	defaultMaxOpenShardIndexes = 512
	defaultIndexCacheMinIdle   = 30 * time.Second
)

func New(cfg Config) *Server {
	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())

	return &Server{
		nodeID:           cfg.NodeID,
		listen:           cfg.Listen,
		publicAddr:       cfg.PublicAddr,
		dataDir:          cfg.DataDir,
		mode:             cfg.Mode,
		backgroundCtx:    backgroundCtx,
		backgroundCancel: backgroundCancel,
		client: &http.Client{
			Timeout: 8 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   3 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		indexes:                map[string]bleve.Index{},
		replicaCache:           map[string]string{},
		shardSyncPending:       map[string]int64{},
		shardSyncingVersion:    map[string]int64{},
		shardSyncedVersion:     map[string]int64{},
		etcdEndpoints:          append([]string(nil), cfg.ETCDEndpoints...),
		routing:                map[string]RoutingEntry{},
		partitionShardCounts:   map[string]int{},
		routingByIndexDay:      map[string][]RoutingEntry{},
		routingByDay:           map[string][]RoutingEntry{},
		members:                map[string]NodeInfo{},
		drainStates:            map[string]NodeDrainState{},
		offlineStates:          map[string]NodeOfflineState{},
		indexRetentionPolicies: map[string]IndexRetentionPolicy{},
		replicaRepairStates:    map[string]ReplicaRepairState{},
		replicaRepairRunning:   map[string]bool{},
		replicaRepairRequests:  map[string]int64{},
		replicationFactor:      cfg.ReplicationFactor,
		defaultShardsPerDay:    normalizedDefaultShardsPerDay(cfg.DefaultShardsPerDay),
		shardSyncConcurrency:   cfg.ShardSyncConcurrency,
		searchAdmission:        make(chan struct{}, defaultSearchRequestLimit),
		maxOpenShardIndexes:    defaultMaxOpenShardIndexes,
		indexCacheMinIdle:      defaultIndexCacheMinIdle,
		routingPrefix:          "/distsearch/routing/",
		memberPrefix:           "/distsearch/members/",
		drainPrefix:            "/distsearch/drain/",
		offlinePrefix:          "/distsearch/offline/",
		indexRetentionPrefix:   "/distsearch/index-retention/",
		replicaRepairPrefix:    "/distsearch/replica-repair/",
		indexLastAccess:        map[string]time.Time{},
	}
}

func normalizedDefaultShardsPerDay(value int) int {
	if value > 0 {
		return value
	}
	return defaultShardsPerDay
}

func (s *Server) Run() error {
	ctx := s.backgroundCtx
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

	go s.watchMembers(s.backgroundCtx)
	go s.watchDrainStates(s.backgroundCtx)
	go s.watchOfflineStates(s.backgroundCtx)
	go s.watchRouting(s.backgroundCtx)
	go s.watchIndexRetentionPolicies(s.backgroundCtx)
	go s.watchReplicaRepairStates(s.backgroundCtx)
	go s.offlineDrainLoop(s.backgroundCtx)
	go s.retentionCleanupLoop(s.backgroundCtx)

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	httpServer := &http.Server{
		Addr:              s.listen,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
	s.setHTTPServer(httpServer)
	defer s.clearHTTPServer(httpServer)

	log.Printf("starting mode=%s node=%s listen=%s etcd=%v", s.mode, s.nodeID, s.listen, s.etcdEndpoints)
	err := httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.backgroundCancel != nil {
			s.backgroundCancel()
		}
		if s.memberLeaseCancel != nil {
			s.memberLeaseCancel()
		}
		if httpServer := s.currentHTTPServer(); httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
			if err := httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				_ = httpServer.Close()
			}
			cancel()
		}
		s.mu.Lock()
		for _, idx := range s.indexes {
			_ = idx.Close()
		}
		s.mu.Unlock()
		if s.etcd != nil {
			_ = s.etcd.Close()
		}
	})
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
	mux.HandleFunc("/internal/fetch_docs", s.handleFetchDocs)
	mux.HandleFunc("/internal/dump_docs", s.handleDumpDocs)
	mux.HandleFunc("/internal/stream_docs", s.handleStreamDocs)
	mux.HandleFunc("/internal/snapshot_shard", s.handleSnapshotShard)
	mux.HandleFunc("/internal/install_snapshot_shard", s.handleInstallSnapshotShard)
	mux.HandleFunc("/internal/shard_stats", s.handleShardStats)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func (s *Server) setHTTPServer(httpServer *http.Server) {
	s.httpServerMu.Lock()
	s.httpServer = httpServer
	s.httpServerMu.Unlock()
}

func (s *Server) clearHTTPServer(httpServer *http.Server) {
	s.httpServerMu.Lock()
	if s.httpServer == httpServer {
		s.httpServer = nil
	}
	s.httpServerMu.Unlock()
}

func (s *Server) currentHTTPServer() *http.Server {
	s.httpServerMu.Lock()
	defer s.httpServerMu.Unlock()
	return s.httpServer
}
