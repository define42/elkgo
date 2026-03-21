package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	rebalanceElectionPath     = "/distsearch/admin/rebalance"
	shardSyncConcurrency      = 4
	shardSyncBatchSize        = 500
	shardSyncTimeout          = 5 * time.Minute
	offlineDrainGracePeriod   = 15 * time.Minute
	onlineResumeGracePeriod   = 15 * time.Minute
	offlineDrainCheckInterval = time.Minute
)

type shardSyncTask struct {
	current  RoutingEntry
	previous RoutingEntry
}

type routingGroup struct {
	indexName string
	day       string
	rf        int
	byShard   map[int]RoutingEntry
}

func (s *Server) maybeRebalanceRouting(ctx context.Context) {
	if !s.isCoordinatorMode() {
		return
	}

	routes := s.snapshotRouting()
	if len(routes) == 0 {
		return
	}

	if _, err := s.maybeAutoDrainExpiredOfflineNodes(ctx); err != nil {
		log.Printf("auto-drain check failed before rebalance: %v", err)
		return
	}
	if _, err := s.maybeAutoResumeRecoveredNodes(ctx); err != nil {
		log.Printf("auto-resume check failed before rebalance: %v", err)
		return
	}
	if s.hasRecentOfflineNodes(time.Now().UTC()) {
		return
	}

	if err := s.rebalanceRouting(ctx); err != nil {
		log.Printf("rebalance routing failed: %v", err)
	}
}

func (s *Server) rebalanceRouting(ctx context.Context) error {
	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		return err
	}
	defer sess.Close()

	elect := concurrency.NewElection(sess, rebalanceElectionPath)
	campaignCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := elect.Campaign(campaignCtx, s.nodeID); err != nil {
		return fmt.Errorf("rebalance leadership failed: %w", err)
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	members := s.snapshotMembers()
	routes := s.snapshotRouting()
	updates := buildRoutingRebalanceUpdates(members, routes)
	if len(updates) == 0 {
		return nil
	}

	for _, entry := range updates {
		b, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := s.etcd.Put(ctx, s.routingKey(entry.IndexName, entry.Day, entry.ShardID), string(b), clientv3.WithPrevKV()); err != nil {
			return err
		}
	}

	_ = s.loadRouting(context.Background())
	log.Printf("rebalance updated %d shard routes across %d members", len(updates), len(members))
	return nil
}

func (s *Server) maybeAutoDrainExpiredOfflineNodes(ctx context.Context) (bool, error) {
	if !s.isCoordinatorMode() {
		return false, nil
	}

	offlineStates := s.snapshotOfflineStates()
	if len(offlineStates) == 0 {
		return false, nil
	}

	activeMembers := s.snapshotMembers()
	drainStates := s.snapshotDrainStates()
	now := time.Now().UTC()
	changed := false

	nodeIDs := make([]string, 0, len(offlineStates))
	for nodeID := range offlineStates {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		if _, active := activeMembers[nodeID]; active {
			continue
		}
		if _, drained := drainStates[nodeID]; drained {
			continue
		}

		state := offlineStates[nodeID]
		if !offlineStateExpired(state, now) {
			continue
		}

		drainState := NodeDrainState{
			NodeID:      nodeID,
			RequestedAt: now.Format(time.RFC3339),
			Auto:        true,
		}
		b, err := json.Marshal(drainState)
		if err != nil {
			return changed, err
		}
		if _, err := s.etcd.Put(ctx, s.drainPrefix+nodeID, string(b)); err != nil {
			return changed, err
		}
		changed = true
	}

	if changed {
		if err := s.loadMembers(context.Background()); err != nil {
			return true, err
		}
	}

	return changed, nil
}

func (s *Server) maybeAutoResumeRecoveredNodes(ctx context.Context) (bool, error) {
	if !s.isCoordinatorMode() {
		return false, nil
	}

	members := s.snapshotMembers()
	drainStates := s.snapshotDrainStates()
	now := time.Now().UTC()
	resumeNodeIDs := autoResumableNodes(members, drainStates, now)
	if len(resumeNodeIDs) == 0 {
		return false, nil
	}

	for _, nodeID := range resumeNodeIDs {
		if _, err := s.etcd.Delete(ctx, s.drainPrefix+nodeID); err != nil {
			return false, err
		}
	}

	if err := s.loadMembers(context.Background()); err != nil {
		return true, err
	}
	return true, nil
}

func (s *Server) hasRecentOfflineNodes(now time.Time) bool {
	offlineStates := s.snapshotOfflineStates()
	if len(offlineStates) == 0 {
		return false
	}

	activeMembers := s.snapshotMembers()
	drainStates := s.snapshotDrainStates()
	for nodeID, state := range offlineStates {
		if _, active := activeMembers[nodeID]; active {
			continue
		}
		if _, drained := drainStates[nodeID]; drained {
			continue
		}
		if !offlineStateExpired(state, now) {
			return true
		}
	}
	return false
}

func offlineStateExpired(state NodeOfflineState, now time.Time) bool {
	missingSince, err := time.Parse(time.RFC3339, strings.TrimSpace(state.MissingSince))
	if err != nil {
		return true
	}
	return !missingSince.After(now.Add(-offlineDrainGracePeriod))
}

func autoResumableNodes(members map[string]NodeInfo, drainStates map[string]NodeDrainState, now time.Time) []string {
	resumeNodeIDs := make([]string, 0)
	for nodeID, drainState := range drainStates {
		if !drainState.Auto {
			continue
		}
		member, ok := members[nodeID]
		if !ok {
			continue
		}
		if !memberOnlineStable(member, now) {
			continue
		}
		resumeNodeIDs = append(resumeNodeIDs, nodeID)
	}
	sort.Strings(resumeNodeIDs)
	return resumeNodeIDs
}

func memberOnlineStable(member NodeInfo, now time.Time) bool {
	startedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(member.StartedAt))
	if err != nil {
		return false
	}
	return !startedAt.After(now.Add(-onlineResumeGracePeriod))
}

func buildRoutingRebalanceUpdates(members map[string]NodeInfo, routes map[string]RoutingEntry) []RoutingEntry {
	if len(members) == 0 || len(routes) == 0 {
		return nil
	}

	groups := groupRoutingByIndexDay(routes)
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	now := time.Now().UTC()
	versionBase := now.UnixNano()
	updatedAt := now.Format(time.RFC3339)
	updates := make([]RoutingEntry, 0)

	for _, groupKey := range groupKeys {
		group := groups[groupKey]
		nodes := routingCandidateNodes(members, group.rf)
		desired := generateRouting(nodes, enforcedShardsPerDay, group.rf)

		for shardID := 0; shardID < enforcedShardsPerDay; shardID++ {
			desiredReplicas := desired[shardID]
			current, ok := group.byShard[shardID]
			if ok {
				desiredReplicas = preserveReplicaOrder(current.Replicas, desiredReplicas)
			}
			if ok && sameReplicaSet(current.Replicas, desiredReplicas) {
				continue
			}

			entry := RoutingEntry{
				IndexName: group.indexName,
				Day:       group.day,
				ShardID:   shardID,
				Replicas:  append([]string(nil), desiredReplicas...),
				Version:   versionBase + int64(len(updates)+1),
				UpdatedAt: updatedAt,
			}
			if ok {
				entry.Version = versionBase + int64(len(updates)+1)
			}
			updates = append(updates, entry)
		}
	}

	sort.Slice(updates, func(i, j int) bool {
		if updates[i].IndexName == updates[j].IndexName {
			if updates[i].Day == updates[j].Day {
				return updates[i].ShardID < updates[j].ShardID
			}
			return updates[i].Day < updates[j].Day
		}
		return updates[i].IndexName < updates[j].IndexName
	})

	return updates
}

func preserveReplicaOrder(current, desired []string) []string {
	if len(desired) == 0 {
		return nil
	}

	desiredSet := make(map[string]struct{}, len(desired))
	for _, replica := range desired {
		desiredSet[replica] = struct{}{}
	}

	ordered := make([]string, 0, len(desired))
	seen := make(map[string]struct{}, len(desired))
	for _, replica := range current {
		if _, ok := desiredSet[replica]; !ok {
			continue
		}
		if _, ok := seen[replica]; ok {
			continue
		}
		ordered = append(ordered, replica)
		seen[replica] = struct{}{}
	}
	for _, replica := range desired {
		if _, ok := seen[replica]; ok {
			continue
		}
		ordered = append(ordered, replica)
	}
	return ordered
}

func routingCandidateNodes(members map[string]NodeInfo, rf int) []NodeInfo {
	active := make([]NodeInfo, 0, len(members))
	draining := make([]NodeInfo, 0, len(members))
	for _, member := range members {
		if member.DrainRequested {
			draining = append(draining, member)
			continue
		}
		active = append(active, member)
	}

	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	sort.Slice(draining, func(i, j int) bool { return draining[i].ID < draining[j].ID })

	switch {
	case len(active) == 0:
		return append([]NodeInfo(nil), draining...)
	case len(active) >= rf:
		return append([]NodeInfo(nil), active...)
	default:
		out := append([]NodeInfo(nil), active...)
		out = append(out, draining...)
		return out
	}
}

func groupRoutingByIndexDay(routes map[string]RoutingEntry) map[string]routingGroup {
	groups := make(map[string]routingGroup)
	for _, route := range routes {
		key := route.IndexName + "|" + route.Day
		group, ok := groups[key]
		if !ok {
			group = routingGroup{
				indexName: route.IndexName,
				day:       route.Day,
				byShard:   make(map[int]RoutingEntry, enforcedShardsPerDay),
			}
		}
		if len(route.Replicas) > group.rf {
			group.rf = len(route.Replicas)
		}
		group.byShard[route.ShardID] = route
		groups[key] = group
	}
	return groups
}

func sameReplicaSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Server) syncAssignedShardsAsync(oldRouting, newRouting map[string]RoutingEntry) {
	tasks := shardSyncTasksForNode(s.nodeID, oldRouting, newRouting)
	if len(tasks) == 0 {
		return
	}

	go s.syncAssignedShards(context.Background(), tasks)
}

func shardSyncTasksForNode(nodeID string, oldRouting, newRouting map[string]RoutingEntry) []shardSyncTask {
	if len(oldRouting) == 0 || len(newRouting) == 0 {
		return nil
	}

	tasks := make([]shardSyncTask, 0)
	for key, current := range newRouting {
		previous, ok := oldRouting[key]
		if !ok {
			continue
		}
		if routeHasReplica(previous, nodeID) || !routeHasReplica(current, nodeID) {
			continue
		}
		tasks = append(tasks, shardSyncTask{
			current:  current,
			previous: previous,
		})
	}

	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].current.IndexName == tasks[j].current.IndexName {
			if tasks[i].current.Day == tasks[j].current.Day {
				return tasks[i].current.ShardID < tasks[j].current.ShardID
			}
			return tasks[i].current.Day < tasks[j].current.Day
		}
		return tasks[i].current.IndexName < tasks[j].current.IndexName
	})

	return tasks
}

func (s *Server) syncAssignedShards(ctx context.Context, tasks []shardSyncTask) {
	sem := make(chan struct{}, shardSyncConcurrency)
	var wg sync.WaitGroup

	for _, task := range tasks {
		task := task
		if !s.claimShardSync(task.current) {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			err := s.syncShardAssignment(ctx, task)
			s.finishShardSync(task.current, err == nil)
			if err != nil {
				log.Printf("shard sync failed for %s/%s shard %d: %v", task.current.IndexName, task.current.Day, task.current.ShardID, err)
				return
			}
			log.Printf("shard sync complete for %s/%s shard %d", task.current.IndexName, task.current.Day, task.current.ShardID)
		}()
	}

	wg.Wait()
}

func (s *Server) claimShardSync(route RoutingEntry) bool {
	key := routingMapKey(route.IndexName, route.Day, route.ShardID)

	s.shardSyncMu.Lock()
	defer s.shardSyncMu.Unlock()

	if syncedVersion := s.shardSyncedVersion[key]; syncedVersion >= route.Version {
		return false
	}
	if syncingVersion := s.shardSyncingVersion[key]; syncingVersion >= route.Version {
		return false
	}

	s.shardSyncingVersion[key] = route.Version
	return true
}

func (s *Server) finishShardSync(route RoutingEntry, success bool) {
	key := routingMapKey(route.IndexName, route.Day, route.ShardID)

	s.shardSyncMu.Lock()
	defer s.shardSyncMu.Unlock()

	delete(s.shardSyncingVersion, key)
	if success && s.shardSyncedVersion[key] < route.Version {
		s.shardSyncedVersion[key] = route.Version
	}
}

func (s *Server) syncShardAssignment(ctx context.Context, task shardSyncTask) error {
	if !s.ownsReplica(task.current.IndexName, task.current.Day, task.current.ShardID) {
		return nil
	}

	docs, sourceNodeID, err := s.fetchShardDocuments(ctx, task)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		return nil
	}

	if err := s.restoreShardDocuments(task.current, docs); err != nil {
		return err
	}

	log.Printf("shard sync restored %d docs for %s/%s shard %d from %s", len(docs), task.current.IndexName, task.current.Day, task.current.ShardID, sourceNodeID)
	return nil
}

func (s *Server) fetchShardDocuments(ctx context.Context, task shardSyncTask) ([]Document, string, error) {
	candidates := sourceReplicaCandidates(task.previous, task.current, s.nodeID)
	errorsOut := make([]string, 0, len(candidates))

	requestURLSuffix := fmt.Sprintf(
		"/internal/dump_docs?index=%s&day=%s&shard=%d",
		url.QueryEscape(task.current.IndexName),
		url.QueryEscape(task.current.Day),
		task.current.ShardID,
	)

	for _, nodeID := range candidates {
		addr, ok := s.memberAddr(nodeID)
		if !ok {
			errorsOut = append(errorsOut, nodeID+": not registered")
			continue
		}

		var dump DumpDocsResponse
		if err := s.getJSONWithTimeout(ctx, addr+requestURLSuffix, shardSyncTimeout, &dump); err != nil {
			errorsOut = append(errorsOut, nodeID+": "+err.Error())
			continue
		}
		return dump.Docs, nodeID, nil
	}

	if len(errorsOut) == 0 {
		return nil, "", fmt.Errorf("no source replicas available")
	}
	return nil, "", fmt.Errorf("%s", strings.Join(errorsOut, "; "))
}

func sourceReplicaCandidates(previous, current RoutingEntry, selfNodeID string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(previous.Replicas)+len(current.Replicas))

	appendReplica := func(nodeID string) {
		if nodeID == "" || nodeID == selfNodeID {
			return
		}
		if _, ok := seen[nodeID]; ok {
			return
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}

	for _, nodeID := range previous.Replicas {
		appendReplica(nodeID)
	}
	for _, nodeID := range current.Replicas {
		appendReplica(nodeID)
	}

	return out
}

func (s *Server) restoreShardDocuments(route RoutingEntry, docs []Document) error {
	for start := 0; start < len(docs); start += shardSyncBatchSize {
		end := start + shardSyncBatchSize
		if end > len(docs) {
			end = len(docs)
		}

		items := make([]internalIndexBatchItem, 0, end-start)
		for _, doc := range docs[start:end] {
			docCopy := cloneDocument(doc)
			docID := strings.TrimSpace(fmt.Sprint(docCopy["id"]))
			if docID == "" {
				return fmt.Errorf("document missing id")
			}
			docCopy["id"] = docID
			if _, ok := docCopy["partition_day"]; !ok {
				docCopy["partition_day"] = route.Day
			}
			items = append(items, internalIndexBatchItem{
				DocID: docID,
				Doc:   docCopy,
			})
		}

		if err := s.indexBatchLocal(route.IndexName, route.Day, route.ShardID, items); err != nil {
			return err
		}
	}

	return nil
}

func cloneDocument(doc Document) Document {
	out := make(Document, len(doc))
	for key, value := range doc {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			out[key] = cloneValue(nested)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, nested := range typed {
			out[i] = cloneValue(nested)
		}
		return out
	default:
		return typed
	}
}
