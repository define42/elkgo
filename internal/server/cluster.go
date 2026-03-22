package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func (s *Server) connectEtcd(_ context.Context) error {
	cli, err := clientv3.New(clientv3.Config{Endpoints: s.etcdEndpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	s.etcd = cli
	return nil
}

func (s *Server) registerMember(ctx context.Context) error {
	lease, err := s.etcd.Grant(ctx, 15)
	if err != nil {
		return err
	}
	s.memberLeaseID = lease.ID
	member := MemberLease{NodeID: s.nodeID, Addr: s.advertisedAddr(), StartedAt: time.Now().UTC().Format(time.RFC3339)}
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

func (s *Server) loadMembers(ctx context.Context) error {
	resp, err := s.etcd.Get(ctx, s.memberPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	drainResp, err := s.etcd.Get(ctx, s.drainPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	drained := make(map[string]NodeDrainState, len(drainResp.Kvs))
	for _, kv := range drainResp.Kvs {
		var state NodeDrainState
		if err := json.Unmarshal(kv.Value, &state); err != nil {
			continue
		}
		drained[state.NodeID] = state
	}

	members := map[string]NodeInfo{}
	for _, kv := range resp.Kvs {
		var m MemberLease
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			continue
		}
		_, drainRequested := drained[m.NodeID]
		members[m.NodeID] = NodeInfo{
			ID:             m.NodeID,
			Addr:           strings.TrimRight(m.Addr, "/"),
			StartedAt:      strings.TrimSpace(m.StartedAt),
			DrainRequested: drainRequested,
		}
	}
	s.membersMu.Lock()
	s.members = members
	s.membersMu.Unlock()
	s.drainMu.Lock()
	s.drainStates = drained
	s.drainMu.Unlock()
	s.clearReplicaCache()
	return nil
}

func (s *Server) loadOfflineStates(ctx context.Context) error {
	resp, err := s.etcd.Get(ctx, s.offlinePrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	offline := make(map[string]NodeOfflineState, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var state NodeOfflineState
		if err := json.Unmarshal(kv.Value, &state); err != nil {
			continue
		}
		offline[state.NodeID] = state
	}

	s.offlineMu.Lock()
	s.offlineStates = offline
	s.offlineMu.Unlock()
	return nil
}

func (s *Server) watchMembers(ctx context.Context) {
	previousMembers := s.snapshotMembers()
	if err := s.loadMembers(ctx); err != nil {
		log.Printf("initial load members failed: %v", err)
	} else {
		currentMembers := s.snapshotMembers()
		if err := s.reconcileOfflineMarkers(ctx, previousMembers, currentMembers); err != nil {
			log.Printf("initial reconcile offline markers failed: %v", err)
		}
		if err := s.loadOfflineStates(ctx); err != nil {
			log.Printf("initial load offline states after member update failed: %v", err)
		}
		if shouldRebalanceForMemberChange(previousMembers, currentMembers) {
			go s.maybeRebalanceRouting(s.backgroundCtx)
		}
	}

	watchCh := s.etcd.Watch(ctx, s.memberPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch members error: %v", wr.Err())
			continue
		}
		previousMembers := s.snapshotMembers()
		if err := s.loadMembers(ctx); err != nil {
			log.Printf("load members failed: %v", err)
			continue
		}
		currentMembers := s.snapshotMembers()
		if err := s.reconcileOfflineMarkers(ctx, previousMembers, currentMembers); err != nil {
			log.Printf("reconcile offline markers failed: %v", err)
		}
		if err := s.loadOfflineStates(ctx); err != nil {
			log.Printf("load offline states after member update failed: %v", err)
		}
		if shouldRebalanceForMemberChange(previousMembers, currentMembers) {
			go s.maybeRebalanceRouting(s.backgroundCtx)
		}
	}
}

func (s *Server) watchDrainStates(ctx context.Context) {
	if err := s.loadMembers(ctx); err != nil {
		log.Printf("initial load drain states failed: %v", err)
	}

	watchCh := s.etcd.Watch(ctx, s.drainPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch drain states error: %v", wr.Err())
			continue
		}
		if err := s.loadMembers(ctx); err != nil {
			log.Printf("load members after drain update failed: %v", err)
			continue
		}
		go s.maybeRebalanceRouting(s.backgroundCtx)
	}
}

func (s *Server) watchOfflineStates(ctx context.Context) {
	if err := s.loadOfflineStates(ctx); err != nil {
		log.Printf("initial load offline states failed: %v", err)
	}

	watchCh := s.etcd.Watch(ctx, s.offlinePrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch offline states error: %v", wr.Err())
			continue
		}
		if err := s.loadOfflineStates(ctx); err != nil {
			log.Printf("load offline states failed: %v", err)
		}
	}
}

func (s *Server) offlineDrainLoop(ctx context.Context) {
	ticker := time.NewTicker(offlineDrainCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := s.maybeAutoDrainExpiredOfflineNodes(ctx)
			if err != nil {
				log.Printf("auto-drain offline nodes failed: %v", err)
				continue
			}
			resumed, err := s.maybeAutoResumeRecoveredNodes(ctx)
			if err != nil {
				log.Printf("auto-resume recovered nodes failed: %v", err)
				continue
			}
			changed = changed || resumed
			if changed {
				go s.maybeRebalanceRouting(s.backgroundCtx)
			}
		}
	}
}

func (s *Server) loadRouting(ctx context.Context) error {
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
	partitionShardCounts, routingByIndexDay, routingByDay := buildRoutingLookups(routing)
	s.routingMu.Lock()
	oldRouting := s.routing
	tasks := shardSyncTasksForNode(s.nodeID, oldRouting, routing)
	s.routing = routing
	s.partitionShardCounts = partitionShardCounts
	s.routingByIndexDay = routingByIndexDay
	s.routingByDay = routingByDay
	s.notePendingShardSyncTasks(tasks, routing)
	s.routingMu.Unlock()
	s.clearReplicaCache()
	s.syncAssignedShardsAsync(tasks)
	if err := s.cleanupObsoleteReplicaRepairStates(); err != nil {
		return err
	}
	s.resumeReplicaRepairLoops()
	go func() {
		if err := s.backgroundCtx.Err(); err != nil {
			return
		}
		if err := s.cleanupExpiredLocalShardDays(time.Now().UTC()); err != nil {
			log.Printf("cleanup expired local shards failed: %v", err)
		}
	}()
	return nil
}

func (s *Server) watchRouting(ctx context.Context) {
	if err := s.loadRouting(ctx); err != nil {
		log.Printf("initial load routing failed: %v", err)
	}

	watchCh := s.etcd.Watch(ctx, s.routingPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch routing error: %v", wr.Err())
			continue
		}
		_ = s.loadRouting(ctx)
	}
}

func (s *Server) bootstrapRouting(ctx context.Context, indexName, day string, rf int, shardsPerDay int) ([]RoutingEntry, error) {
	members := s.snapshotMembers()
	if len(members) == 0 {
		return nil, errors.New("no members registered")
	}
	if rf > len(members) {
		rf = len(members)
	}

	nodes := routingCandidateNodes(members, rf)
	if len(nodes) == 0 {
		return nil, errors.New("no routable members available")
	}
	if rf > len(nodes) {
		rf = len(nodes)
	}
	if shardsPerDay <= 0 {
		shardsPerDay = s.defaultShardsPerDay
	}

	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		return nil, err
	}
	defer sess.Close()

	elect := concurrency.NewElection(sess, "/distsearch/admin/bootstrap")
	campaignCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := elect.Campaign(campaignCtx, s.nodeID); err != nil {
		return nil, fmt.Errorf("bootstrap leadership failed: %w", err)
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	routes := generateRouting(nodes, shardsPerDay, rf)
	created := make([]RoutingEntry, 0, len(routes))
	for shardID, replicas := range routes {
		entry := RoutingEntry{
			IndexName: indexName,
			Day:       day,
			ShardID:   shardID,
			Replicas:  replicas,
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		b, _ := json.Marshal(entry)
		if _, err := s.etcd.Put(ctx, s.routingKey(indexName, day, shardID), string(b)); err != nil {
			return nil, err
		}
		created = append(created, entry)
	}
	_ = s.loadRouting(ctx)
	return created, nil
}

func (s *Server) reconcileOfflineMarkers(ctx context.Context, previousMembers, currentMembers map[string]NodeInfo) error {
	now := time.Now().UTC().Format(time.RFC3339)
	offline := s.snapshotOfflineStates()

	for nodeID, member := range previousMembers {
		if _, stillPresent := currentMembers[nodeID]; stillPresent {
			continue
		}
		if _, alreadyMarked := offline[nodeID]; alreadyMarked {
			continue
		}

		state := NodeOfflineState{
			NodeID:       nodeID,
			Addr:         strings.TrimRight(member.Addr, "/"),
			MissingSince: now,
		}
		b, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if _, err := s.etcd.Put(ctx, s.offlinePrefix+nodeID, string(b)); err != nil {
			return err
		}
		offline[nodeID] = state
	}

	for nodeID := range currentMembers {
		if _, markedOffline := offline[nodeID]; !markedOffline {
			continue
		}
		if _, err := s.etcd.Delete(ctx, s.offlinePrefix+nodeID); err != nil {
			return err
		}
		delete(offline, nodeID)
	}

	return nil
}

func (s *Server) ensureOfflineMarkersForMissingRouteReplicas(ctx context.Context) error {
	activeMembers := s.snapshotMembers()
	offline := s.snapshotOfflineStates()
	drained := s.snapshotDrainStates()
	now := time.Now().UTC().Format(time.RFC3339)

	for _, route := range s.snapshotRouting() {
		for _, nodeID := range route.Replicas {
			if nodeID == "" {
				continue
			}
			if _, ok := activeMembers[nodeID]; ok {
				continue
			}
			if _, ok := offline[nodeID]; ok {
				continue
			}
			if _, ok := drained[nodeID]; ok {
				continue
			}

			state := NodeOfflineState{
				NodeID:       nodeID,
				MissingSince: now,
			}
			b, err := json.Marshal(state)
			if err != nil {
				return err
			}
			if _, err := s.etcd.Put(ctx, s.offlinePrefix+nodeID, string(b)); err != nil {
				return err
			}
			offline[nodeID] = state
		}
	}

	return nil
}

func shouldRebalanceForMemberChange(previousMembers, currentMembers map[string]NodeInfo) bool {
	for nodeID, member := range currentMembers {
		previous, ok := previousMembers[nodeID]
		if !ok {
			return true
		}
		if strings.TrimRight(previous.Addr, "/") != strings.TrimRight(member.Addr, "/") {
			return true
		}
	}
	return false
}
