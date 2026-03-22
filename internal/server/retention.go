package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	indexRetentionCleanupElectionPath = "/distsearch/admin/index-retention-cleanup"
	indexRetentionCleanupInterval     = time.Hour
)

func (s *Server) indexRetentionKey(indexName string) string {
	return s.indexRetentionPrefix + indexName
}

func (s *Server) loadIndexRetentionPolicies(ctx context.Context) error {
	if s.etcd == nil {
		return nil
	}

	resp, err := s.etcd.Get(ctx, s.indexRetentionPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	policies := make(map[string]IndexRetentionPolicy, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var policy IndexRetentionPolicy
		if err := json.Unmarshal(kv.Value, &policy); err != nil {
			continue
		}
		indexName := strings.TrimSpace(policy.IndexName)
		if indexName == "" || policy.RetentionDays <= 0 {
			continue
		}
		policy.IndexName = indexName
		policies[indexName] = policy
	}

	s.indexRetentionMu.Lock()
	s.indexRetentionPolicies = policies
	s.indexRetentionMu.Unlock()
	return nil
}

func (s *Server) watchIndexRetentionPolicies(ctx context.Context) {
	if s.etcd == nil {
		return
	}

	watchCh := s.etcd.Watch(ctx, s.indexRetentionPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch index retention policies error: %v", wr.Err())
			continue
		}
		if err := s.loadIndexRetentionPolicies(context.Background()); err != nil {
			log.Printf("load index retention policies failed: %v", err)
			continue
		}
		s.runRetentionCleanupAsync()
	}
}

func (s *Server) snapshotIndexRetentionPolicies() map[string]IndexRetentionPolicy {
	s.indexRetentionMu.RLock()
	defer s.indexRetentionMu.RUnlock()

	out := make(map[string]IndexRetentionPolicy, len(s.indexRetentionPolicies))
	for indexName, policy := range s.indexRetentionPolicies {
		out[indexName] = policy
	}
	return out
}

func (s *Server) getIndexRetentionPolicy(indexName string) (IndexRetentionPolicy, bool) {
	s.indexRetentionMu.RLock()
	defer s.indexRetentionMu.RUnlock()

	policy, ok := s.indexRetentionPolicies[strings.TrimSpace(indexName)]
	return policy, ok
}

func (s *Server) setIndexRetentionPolicy(ctx context.Context, indexName string, retentionDays int) (IndexRetentionPolicy, error) {
	if s.etcd == nil {
		return IndexRetentionPolicy{}, errors.New("etcd not configured")
	}

	indexName = strings.TrimSpace(indexName)
	if indexName == "" {
		return IndexRetentionPolicy{}, errors.New("missing index")
	}
	if retentionDays <= 0 {
		return IndexRetentionPolicy{}, errors.New("retention_days must be > 0")
	}

	policy := IndexRetentionPolicy{
		IndexName:     indexName,
		RetentionDays: retentionDays,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	body, err := json.Marshal(policy)
	if err != nil {
		return IndexRetentionPolicy{}, err
	}
	if _, err := s.etcd.Put(ctx, s.indexRetentionKey(indexName), string(body)); err != nil {
		return IndexRetentionPolicy{}, err
	}

	s.indexRetentionMu.Lock()
	s.indexRetentionPolicies[indexName] = policy
	s.indexRetentionMu.Unlock()
	return policy, nil
}

func (s *Server) clearIndexRetentionPolicy(ctx context.Context, indexName string) error {
	if s.etcd == nil {
		return errors.New("etcd not configured")
	}

	indexName = strings.TrimSpace(indexName)
	if indexName == "" {
		return errors.New("missing index")
	}
	if _, err := s.etcd.Delete(ctx, s.indexRetentionKey(indexName)); err != nil {
		return err
	}

	s.indexRetentionMu.Lock()
	delete(s.indexRetentionPolicies, indexName)
	s.indexRetentionMu.Unlock()
	return nil
}

func (s *Server) runRetentionCleanup(ctx context.Context, now time.Time) error {
	if err := s.cleanupExpiredRouting(ctx, now); err != nil {
		return err
	}
	return s.cleanupExpiredLocalShardDays(now)
}

func (s *Server) runRetentionCleanupAsync() {
	go func() {
		if err := s.runRetentionCleanup(context.Background(), time.Now().UTC()); err != nil {
			log.Printf("retention cleanup failed: %v", err)
		}
	}()
}

func (s *Server) retentionCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(indexRetentionCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.runRetentionCleanup(context.Background(), time.Now().UTC()); err != nil {
				log.Printf("retention cleanup failed: %v", err)
			}
		}
	}
}

func (s *Server) cleanupExpiredRouting(ctx context.Context, now time.Time) error {
	if !s.isCoordinatorMode() || s.etcd == nil {
		return nil
	}

	expiredPrefixes := s.expiredRoutingPrefixes(now)
	if len(expiredPrefixes) == 0 {
		return nil
	}

	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		return err
	}
	defer sess.Close()

	elect := concurrency.NewElection(sess, indexRetentionCleanupElectionPath)
	campaignCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := elect.Campaign(campaignCtx, s.nodeID); err != nil {
		return fmt.Errorf("retention cleanup leadership failed: %w", err)
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	removed := 0
	for _, prefix := range expiredPrefixes {
		resp, err := s.etcd.Delete(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		removed += int(resp.Deleted)
	}
	if removed == 0 {
		return nil
	}

	return s.loadRouting(context.Background())
}

func (s *Server) expiredRoutingPrefixes(now time.Time) []string {
	policies := s.snapshotIndexRetentionPolicies()
	if len(policies) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	prefixes := make([]string, 0)
	for _, route := range s.snapshotRouting() {
		policy, ok := policies[route.IndexName]
		if !ok || !indexDayExpired(route.Day, policy.RetentionDays, now) {
			continue
		}
		prefix := s.routingPrefix + route.IndexName + "/" + route.Day + "/"
		if _, ok := seen[prefix]; ok {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	return prefixes
}

func (s *Server) cleanupExpiredLocalShardDays(now time.Time) error {
	policies := s.snapshotIndexRetentionPolicies()
	if len(policies) == 0 {
		return nil
	}

	root := filepath.Join(s.dataDir, s.nodeID)
	indexEntries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	routes := s.snapshotRouting()
	for _, indexEntry := range indexEntries {
		if !indexEntry.IsDir() {
			continue
		}

		indexName := indexEntry.Name()
		policy, ok := policies[indexName]
		if !ok {
			continue
		}

		indexPath := filepath.Join(root, indexName)
		dayEntries, err := os.ReadDir(indexPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}

		for _, dayEntry := range dayEntries {
			if !dayEntry.IsDir() {
				continue
			}

			day := dayEntry.Name()
			if !indexDayExpired(day, policy.RetentionDays, now) {
				continue
			}
			if routingExistsForIndexDay(routes, indexName, day) {
				continue
			}
			if err := s.removeLocalShardDay(indexName, day); err != nil {
				return err
			}
		}
	}

	return nil
}

func routingExistsForIndexDay(routes map[string]RoutingEntry, indexName, day string) bool {
	for _, route := range routes {
		if route.IndexName == indexName && route.Day == day {
			return true
		}
	}
	return false
}

func indexDayExpired(day string, retentionDays int, now time.Time) bool {
	if retentionDays <= 0 {
		return false
	}

	parsedDay, err := time.Parse("2006-01-02", strings.TrimSpace(day))
	if err != nil {
		return false
	}

	today := utcDayStart(now)
	cutoff := today.AddDate(0, 0, -(retentionDays - 1))
	return parsedDay.Before(cutoff)
}

func utcDayStart(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}
