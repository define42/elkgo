package server

import (
	"context"
	"net/http"
	"sync"

	"github.com/blevesearch/bleve/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

type internalIndexBatchItem struct {
	DocID string   `json:"doc_id"`
	Doc   Document `json:"doc"`
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
}

type RoutingEntryStats struct {
	RoutingEntry
	EventCount uint64 `json:"event_count"`
	CountError string `json:"count_error,omitempty"`
}

type Config struct {
	Mode              string
	NodeID            string
	Listen            string
	PublicAddr        string
	DataDir           string
	ETCDEndpoints     []string
	ReplicationFactor int
}

type Server struct {
	nodeID     string
	listen     string
	publicAddr string
	dataDir    string
	mode       string

	client *http.Client

	mu      sync.RWMutex
	indexes map[string]bleve.Index

	replicaCacheMu sync.RWMutex
	replicaCache   map[string]string

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
