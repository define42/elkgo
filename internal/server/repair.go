package server

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	replicaRepairRetryDelay    = 500 * time.Millisecond
	replicaRepairMaxRetryDelay = 10 * time.Second
	replicaRepairRequestTTL    = 5 * time.Second
)

func replicaRepairMapKey(indexName, day string, shardID int, nodeID string) string {
	return routingMapKey(indexName, day, shardID) + "|" + nodeID
}

func (s *Server) replicaRepairKey(indexName, day string, shardID int, nodeID string) string {
	return s.replicaRepairPrefix + indexName + "/" + day + "/" + strconv.Itoa(shardID) + "/" + nodeID
}

func (s *Server) loadReplicaRepairStates(ctx context.Context) error {
	if s.etcd == nil {
		return nil
	}

	resp, err := s.etcd.Get(ctx, s.replicaRepairPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	states := make(map[string]ReplicaRepairState, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var state ReplicaRepairState
		if err := json.Unmarshal(kv.Value, &state); err != nil {
			continue
		}
		states[replicaRepairMapKey(state.IndexName, state.Day, state.ShardID, state.NodeID)] = state
	}

	s.replicaRepairMu.Lock()
	s.replicaRepairStates = states
	s.replicaRepairMu.Unlock()
	s.clearReplicaCache()
	return nil
}

func (s *Server) watchReplicaRepairStates(ctx context.Context) {
	if s.etcd == nil {
		return
	}

	if err := s.loadReplicaRepairStates(ctx); err != nil {
		log.Printf("initial load replica repair states failed: %v", err)
	}

	watchCh := s.etcd.Watch(ctx, s.replicaRepairPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch replica repair states error: %v", wr.Err())
			continue
		}
		if err := s.loadReplicaRepairStates(ctx); err != nil {
			log.Printf("load replica repair states failed: %v", err)
			continue
		}
		if err := s.cleanupObsoleteReplicaRepairStates(); err != nil {
			log.Printf("cleanup obsolete replica repair states failed: %v", err)
			continue
		}
		s.resumeReplicaRepairLoops()
	}
}

func (s *Server) snapshotReplicaRepairStates() map[string]ReplicaRepairState {
	s.replicaRepairMu.RLock()
	defer s.replicaRepairMu.RUnlock()

	out := make(map[string]ReplicaRepairState, len(s.replicaRepairStates))
	for key, state := range s.replicaRepairStates {
		out[key] = state
	}
	return out
}

func (s *Server) replicaNeedsRepair(indexName, day string, shardID int, nodeID string) bool {
	s.replicaRepairMu.RLock()
	defer s.replicaRepairMu.RUnlock()
	_, ok := s.replicaRepairStates[replicaRepairMapKey(indexName, day, shardID, nodeID)]
	return ok
}

func (s *Server) markReplicaRepairState(route RoutingEntry, nodeID string) error {
	state := ReplicaRepairState{
		IndexName: route.IndexName,
		Day:       route.Day,
		ShardID:   route.ShardID,
		NodeID:    nodeID,
		MarkedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	s.replicaRepairMu.Lock()
	s.replicaRepairStates[replicaRepairMapKey(route.IndexName, route.Day, route.ShardID, nodeID)] = state
	s.replicaRepairMu.Unlock()
	s.invalidateReplica(routingMapKey(route.IndexName, route.Day, route.ShardID), nodeID)

	if s.etcd == nil {
		return nil
	}

	b, err := json.Marshal(state)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.backgroundCtx, replicaRepairRequestTTL)
	defer cancel()
	_, err = s.etcd.Put(ctx, s.replicaRepairKey(route.IndexName, route.Day, route.ShardID, nodeID), string(b))
	return err
}

func (s *Server) clearReplicaRepairState(indexName, day string, shardID int, nodeID string) error {
	if s.etcd != nil {
		ctx, cancel := context.WithTimeout(s.backgroundCtx, replicaRepairRequestTTL)
		defer cancel()
		if _, err := s.etcd.Delete(ctx, s.replicaRepairKey(indexName, day, shardID, nodeID)); err != nil {
			return err
		}
	}

	s.replicaRepairMu.Lock()
	delete(s.replicaRepairStates, replicaRepairMapKey(indexName, day, shardID, nodeID))
	s.replicaRepairMu.Unlock()
	s.invalidateReplica(routingMapKey(indexName, day, shardID), nodeID)
	return nil
}

func (s *Server) cleanupObsoleteReplicaRepairStates() error {
	for _, state := range s.snapshotReplicaRepairStates() {
		route, ok := s.getRouting(state.IndexName, state.Day, state.ShardID)
		if ok && routeHasReplica(route, state.NodeID) {
			continue
		}
		if err := s.clearReplicaRepairState(state.IndexName, state.Day, state.ShardID, state.NodeID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) scheduleReplicaRepair(route RoutingEntry, nodeID string) {
	if nodeID == "" || nodeID == s.nodeID {
		return
	}

	if err := s.markReplicaRepairState(route, nodeID); err != nil {
		log.Printf("mark replica repair state failed for %s/%s shard %d replica %s: %v", route.IndexName, route.Day, route.ShardID, nodeID, err)
	}

	key := replicaRepairMapKey(route.IndexName, route.Day, route.ShardID, nodeID)
	s.noteReplicaRepairRequest(key)
	if !s.claimReplicaRepairWorker(key) {
		return
	}

	go s.repairReplicaLoop(route.IndexName, route.Day, route.ShardID, nodeID)
}

func (s *Server) resumeReplicaRepairLoops() {
	for _, state := range s.snapshotReplicaRepairStates() {
		route, ok := s.getRouting(state.IndexName, state.Day, state.ShardID)
		if !ok || !routeHasReplica(route, state.NodeID) {
			if err := s.clearReplicaRepairState(state.IndexName, state.Day, state.ShardID, state.NodeID); err != nil {
				log.Printf("clear obsolete replica repair state failed for %s/%s shard %d replica %s: %v", state.IndexName, state.Day, state.ShardID, state.NodeID, err)
			}
			continue
		}
		if len(route.Replicas) == 0 || route.Replicas[0] != s.nodeID {
			continue
		}

		key := replicaRepairMapKey(state.IndexName, state.Day, state.ShardID, state.NodeID)
		s.noteReplicaRepairRequest(key)
		if !s.claimReplicaRepairWorker(key) {
			continue
		}

		go s.repairReplicaLoop(state.IndexName, state.Day, state.ShardID, state.NodeID)
	}
}

func (s *Server) noteReplicaRepairRequest(key string) int64 {
	requestedAt := time.Now().UnixNano()

	s.replicaRepairTaskMu.Lock()
	s.replicaRepairRequests[key] = requestedAt
	s.replicaRepairTaskMu.Unlock()

	return requestedAt
}

func (s *Server) currentReplicaRepairRequest(key string) int64 {
	s.replicaRepairTaskMu.Lock()
	defer s.replicaRepairTaskMu.Unlock()

	requestedAt := s.replicaRepairRequests[key]
	if requestedAt == 0 {
		requestedAt = time.Now().UnixNano()
		s.replicaRepairRequests[key] = requestedAt
	}
	return requestedAt
}

func (s *Server) latestReplicaRepairRequest(key string) int64 {
	s.replicaRepairTaskMu.Lock()
	defer s.replicaRepairTaskMu.Unlock()
	return s.replicaRepairRequests[key]
}

func (s *Server) clearReplicaRepairRequest(key string) {
	s.replicaRepairTaskMu.Lock()
	delete(s.replicaRepairRequests, key)
	s.replicaRepairTaskMu.Unlock()
}

func (s *Server) claimReplicaRepairWorker(key string) bool {
	s.replicaRepairTaskMu.Lock()
	defer s.replicaRepairTaskMu.Unlock()

	if s.replicaRepairRunning[key] {
		return false
	}

	s.replicaRepairRunning[key] = true
	return true
}

func (s *Server) releaseReplicaRepairWorker(key string) {
	s.replicaRepairTaskMu.Lock()
	delete(s.replicaRepairRunning, key)
	s.replicaRepairTaskMu.Unlock()
}

func (s *Server) repairReplicaLoop(indexName, day string, shardID int, nodeID string) {
	key := replicaRepairMapKey(indexName, day, shardID, nodeID)
	defer s.releaseReplicaRepairWorker(key)

	delay := replicaRepairRetryDelay
	for {
		if err := s.backgroundCtx.Err(); err != nil {
			return
		}

		route, ok := s.getRouting(indexName, day, shardID)
		if !ok || !routeHasReplica(route, nodeID) {
			if err := s.clearReplicaRepairState(indexName, day, shardID, nodeID); err != nil {
				log.Printf("clear replica repair state failed for %s/%s shard %d replica %s: %v", indexName, day, shardID, nodeID, err)
			}
			s.clearReplicaRepairRequest(key)
			return
		}
		if len(route.Replicas) == 0 || route.Replicas[0] != s.nodeID {
			return
		}

		requestedAt := s.currentReplicaRepairRequest(key)
		if err := s.repairReplica(route, nodeID); err != nil {
			log.Printf("replica repair failed for %s/%s shard %d replica %s: %v", indexName, day, shardID, nodeID, err)
			if !sleepWithContext(s.backgroundCtx, delay) {
				return
			}
			if delay < replicaRepairMaxRetryDelay {
				delay *= 2
				if delay > replicaRepairMaxRetryDelay {
					delay = replicaRepairMaxRetryDelay
				}
			}
			continue
		}

		if s.latestReplicaRepairRequest(key) > requestedAt {
			delay = replicaRepairRetryDelay
			continue
		}
		if err := s.clearReplicaRepairState(indexName, day, shardID, nodeID); err != nil {
			log.Printf("clear replica repair state failed for %s/%s shard %d replica %s: %v", indexName, day, shardID, nodeID, err)
			if !sleepWithContext(s.backgroundCtx, delay) {
				return
			}
			continue
		}

		s.clearReplicaRepairRequest(key)
		return
	}
}

func (s *Server) repairReplica(route RoutingEntry, nodeID string) error {
	ctx, cancel := context.WithTimeout(s.backgroundCtx, shardSyncTimeout)
	defer cancel()

	if err := s.transferShardSnapshotToReplica(ctx, route, nodeID); err == nil {
		return nil
	}

	_, err := s.streamShardToReplica(route, nodeID)
	return err
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
