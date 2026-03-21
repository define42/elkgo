package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
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
	members := map[string]NodeInfo{}
	for _, kv := range resp.Kvs {
		var m MemberLease
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			continue
		}
		members[m.NodeID] = NodeInfo{ID: m.NodeID, Addr: strings.TrimRight(m.Addr, "/")}
	}
	s.membersMu.Lock()
	s.members = members
	s.membersMu.Unlock()
	return nil
}

func (s *Server) watchMembers(ctx context.Context) {
	watchCh := s.etcd.Watch(ctx, s.memberPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch members error: %v", wr.Err())
			continue
		}
		_ = s.loadMembers(context.Background())
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
	s.routingMu.Lock()
	s.routing = routing
	s.routingMu.Unlock()
	return nil
}

func (s *Server) watchRouting(ctx context.Context) {
	watchCh := s.etcd.Watch(ctx, s.routingPrefix, clientv3.WithPrefix())
	for wr := range watchCh {
		if wr.Err() != nil {
			log.Printf("watch routing error: %v", wr.Err())
			continue
		}
		_ = s.loadRouting(context.Background())
	}
}

func (s *Server) bootstrapRouting(ctx context.Context, indexName, day string, rf int) ([]RoutingEntry, error) {
	members := s.snapshotMembers()
	if len(members) == 0 {
		return nil, errors.New("no members registered")
	}
	if rf > len(members) {
		rf = len(members)
	}

	nodes := make([]NodeInfo, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, m)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

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

	routes := generateRouting(nodes, enforcedShardsPerDay, rf)
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
	_ = s.loadRouting(context.Background())
	return created, nil
}
