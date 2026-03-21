package server

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func (s *Server) routeForDoc(indexName, day, docID string) (int, RoutingEntry, error) {
	shardID := keyToShard(docID, enforcedShardsPerDay)
	rt, ok := s.getRouting(indexName, day, shardID)
	if !ok {
		return 0, RoutingEntry{}, fmt.Errorf("no routing for %s/%s shard %d", indexName, day, shardID)
	}
	return shardID, rt, nil
}

func (s *Server) routingKey(indexName, day string, shardID int) string {
	return s.routingPrefix + indexName + "/" + day + "/" + strconv.Itoa(shardID)
}

func routingMapKey(indexName, day string, shardID int) string {
	return indexName + "|" + day + "|" + strconv.Itoa(shardID)
}

func partitionKey(indexName, day string, shardID int) string {
	return routingMapKey(indexName, day, shardID)
}

func (s *Server) getRouting(indexName, day string, shardID int) (RoutingEntry, bool) {
	s.routingMu.RLock()
	defer s.routingMu.RUnlock()
	rt, ok := s.routing[routingMapKey(indexName, day, shardID)]
	return rt, ok
}

func (s *Server) snapshotRouting() map[string]RoutingEntry {
	s.routingMu.RLock()
	defer s.routingMu.RUnlock()
	out := make(map[string]RoutingEntry, len(s.routing))
	for k, v := range s.routing {
		out[k] = v
	}
	return out
}

func (s *Server) snapshotMembers() map[string]NodeInfo {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	out := make(map[string]NodeInfo, len(s.members))
	for k, v := range s.members {
		out[k] = v
	}
	return out
}

func (s *Server) memberAddr(nodeID string) (string, bool) {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	m, ok := s.members[nodeID]
	return m.Addr, ok
}

func (s *Server) ownsReplica(indexName, day string, shardID int) bool {
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

func (s *Server) pickHealthyReplica(ctx context.Context, indexName, day string, shardID int) (string, error) {
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

func (s *Server) advertisedAddr() string {
	if strings.TrimSpace(s.publicAddr) != "" {
		return publicAddrFromListen(s.publicAddr)
	}
	return publicAddrFromListen(s.listen)
}

func (s *Server) isCoordinatorMode() bool {
	return s.mode == "coordinator" || s.mode == "both"
}
