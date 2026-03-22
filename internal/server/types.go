package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Document map[string]interface{}

type NodeInfo struct {
	ID             string `json:"id"`
	Addr           string `json:"addr"`
	StartedAt      string `json:"started_at,omitempty"`
	DrainRequested bool   `json:"drain_requested"`
}

const (
	defaultShardsPerDay  = 48
	enforcedShardsPerDay = defaultShardsPerDay
)

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

type NodeDrainState struct {
	NodeID      string `json:"node_id"`
	RequestedAt string `json:"requested_at"`
	Auto        bool   `json:"auto,omitempty"`
}

type NodeOfflineState struct {
	NodeID       string `json:"node_id"`
	Addr         string `json:"addr"`
	MissingSince string `json:"missing_since"`
}

type ReplicaRepairState struct {
	IndexName string `json:"index_name"`
	Day       string `json:"day"`
	ShardID   int    `json:"shard_id"`
	NodeID    string `json:"node_id"`
	MarkedAt  string `json:"marked_at"`
}

type IndexRetentionPolicy struct {
	IndexName     string `json:"index_name"`
	RetentionDays int    `json:"retention_days"`
	UpdatedAt     string `json:"updated_at"`
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
	FetchDocs bool   `json:"fetch_docs,omitempty"`
}

type SearchShardResponse struct {
	Hits []ShardHit `json:"hits"`
}

type FetchDocsRequest struct {
	IndexName string   `json:"index_name"`
	Day       string   `json:"day"`
	ShardID   int      `json:"shard_id"`
	DocIDs    []string `json:"doc_ids"`
}

type FetchedDocument struct {
	DocID  string   `json:"doc_id"`
	Source Document `json:"source"`
}

type FetchDocsResponse struct {
	Docs []FetchedDocument `json:"docs"`
}

type internalIndexRequest struct {
	IndexName string   `json:"index_name"`
	Day       string   `json:"day"`
	ShardID   int      `json:"shard_id"`
	DocID     string   `json:"doc_id"`
	Doc       Document `json:"doc"`
}

type internalIndexBatchItem struct {
	DocID string          `json:"doc_id"`
	Doc   Document        `json:"doc,omitempty"`
	Raw   json.RawMessage `json:"raw,omitempty"`
}

type internalIndexBatchRequest struct {
	IndexName string                   `json:"index_name"`
	Day       string                   `json:"day"`
	ShardID   int                      `json:"shard_id"`
	Items     []internalIndexBatchItem `json:"items"`
	Replicate bool                     `json:"replicate"`
}

type internalIndexBatchResponse struct {
	OK       bool     `json:"ok"`
	Index    string   `json:"index"`
	Day      string   `json:"day"`
	Shard    int      `json:"shard"`
	Primary  string   `json:"primary,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
	Indexed  int      `json:"indexed"`
	Acks     int      `json:"acks,omitempty"`
	Quorum   int      `json:"quorum,omitempty"`
	Errors   []string `json:"errors,omitempty"`
}

type DumpDocsResponse struct {
	Docs []Document `json:"docs"`
}

type ShardStatsResponse struct {
	IndexName  string `json:"index_name"`
	Day        string `json:"day"`
	ShardID    int    `json:"shard_id"`
	EventCount uint64 `json:"event_count"`
	SizeBytes  uint64 `json:"size_bytes"`
}

type RoutingEntryStats struct {
	RoutingEntry
	EventCount uint64 `json:"event_count"`
	SizeBytes  uint64 `json:"size_bytes"`
	CountError string `json:"count_error,omitempty"`
}

type Config struct {
	Mode                 string
	NodeID               string
	Listen               string
	PublicAddr           string
	DataDir              string
	ETCDEndpoints        []string
	ReplicationFactor    int
	DefaultShardsPerDay  int
	ShardSyncConcurrency int
}

type Server struct {
	nodeID     string
	listen     string
	publicAddr string
	dataDir    string
	mode       string

	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc

	closeOnce sync.Once

	client *http.Client

	httpServerMu sync.Mutex
	httpServer   *http.Server

	mu      sync.RWMutex
	indexes map[string]bleve.Index

	replicaCacheMu sync.RWMutex
	replicaCache   map[string]string

	shardSyncMu         sync.Mutex
	shardSyncingVersion map[string]int64
	shardSyncedVersion  map[string]int64

	etcd              *clientv3.Client
	etcdEndpoints     []string
	memberLeaseID     clientv3.LeaseID
	memberLeaseCancel context.CancelFunc

	drainMu     sync.RWMutex
	drainStates map[string]NodeDrainState

	offlineMu     sync.RWMutex
	offlineStates map[string]NodeOfflineState

	indexRetentionMu       sync.RWMutex
	indexRetentionPolicies map[string]IndexRetentionPolicy

	replicaRepairMu     sync.RWMutex
	replicaRepairStates map[string]ReplicaRepairState

	replicaRepairTaskMu   sync.Mutex
	replicaRepairRunning  map[string]bool
	replicaRepairRequests map[string]int64

	routingMu            sync.RWMutex
	routing              map[string]RoutingEntry
	partitionShardCounts map[string]int
	routingByIndexDay    map[string][]RoutingEntry
	routingByDay         map[string][]RoutingEntry

	membersMu sync.RWMutex
	members   map[string]NodeInfo

	replicationFactor    int
	defaultShardsPerDay  int
	shardSyncConcurrency int
	searchAdmission      chan struct{}
	maxOpenShardIndexes  int
	indexCacheMinIdle    time.Duration
	routingPrefix        string
	memberPrefix         string
	drainPrefix          string
	offlinePrefix        string
	indexRetentionPrefix string
	replicaRepairPrefix  string
	indexLastAccess      map[string]time.Time
}
