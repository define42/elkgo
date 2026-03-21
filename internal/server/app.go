package server

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/blevesearch/bleve/v2"
)

func New(cfg Config) *Server {
	return &Server{
		nodeID:            cfg.NodeID,
		listen:            cfg.Listen,
		publicAddr:        cfg.PublicAddr,
		dataDir:           cfg.DataDir,
		mode:              cfg.Mode,
		addTestData:       cfg.AddTestData,
		client:            &http.Client{Timeout: 8 * time.Second},
		indexes:           map[string]bleve.Index{},
		etcdEndpoints:     append([]string(nil), cfg.ETCDEndpoints...),
		routing:           map[string]RoutingEntry{},
		members:           map[string]NodeInfo{},
		replicationFactor: cfg.ReplicationFactor,
		routingPrefix:     "/distsearch/routing/",
		memberPrefix:      "/distsearch/members/",
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
	if err := s.loadRouting(ctx); err != nil {
		return err
	}

	go s.watchMembers(context.Background())
	go s.watchRouting(context.Background())
	go s.repairLoop(context.Background())
	if s.addTestData {
		go s.ensureTestData(context.Background())
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	log.Printf("starting mode=%s node=%s listen=%s etcd=%v", s.mode, s.nodeID, s.listen, s.etcdEndpoints)
	return http.ListenAndServe(s.listen, loggingMiddleware(mux))
}

func (s *Server) Close() {
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
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/admin/bootstrap", s.handleBootstrap)
	mux.HandleFunc("/admin/indexes", s.handleAvailableIndexes)
	mux.HandleFunc("/admin/routing", s.handleRouting)
	mux.HandleFunc("/index", s.handleIndex)
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/internal/index", s.handleInternalIndex)
	mux.HandleFunc("/internal/search_shard", s.handleSearchShard)
	mux.HandleFunc("/internal/dump_docs", s.handleDumpDocs)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
