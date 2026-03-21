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

const (
	testDataDayCount      = 7
	testDataTotalEvents   = 70000
	testDataEventsPerDay  = testDataTotalEvents / testDataDayCount
	testDataBulkBatchSize = 1000
	testDataMarkerVersion = "v3"
	testDataBulkTimeout   = 5 * time.Minute
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

func (s *Server) ensureTestData(ctx context.Context) {
	const indexName = "events"
	days := testDataDays(time.Now().UTC())
	waitUntil := time.Now().Add(30 * time.Second)
	targetMembers := s.replicationFactor
	if targetMembers < 1 {
		targetMembers = 1
	}
	for len(s.snapshotMembers()) < targetMembers && time.Now().Before(waitUntil) {
		time.Sleep(1 * time.Second)
		_ = s.loadMembers(context.Background())
	}

	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		log.Printf("test data session failed: %v", err)
		return
	}
	defer sess.Close()

	elect := concurrency.NewElection(sess, "/distsearch/admin/test-data")
	campaignCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := elect.Campaign(campaignCtx, s.nodeID); err != nil {
		log.Printf("test data leadership failed: %v", err)
		return
	}
	defer func() { _ = elect.Resign(context.Background()) }()

	seeded := 0
	for _, day := range days {
		markerKey := "/distsearch/admin/test-data-loaded/" + testDataMarkerVersion + "/" + indexName + "/" + day
		if resp, err := s.etcd.Get(ctx, markerKey); err == nil && len(resp.Kvs) > 0 {
			continue
		}

		if _, err := s.bootstrapRouting(ctx, indexName, day, s.replicationFactor); err != nil {
			log.Printf("bootstrap test data routing failed for day %s: %v", day, err)
			return
		}

		indexed, err := s.postDocumentsInBatches(ctx, s.advertisedAddr()+"/bulk?index="+indexName, testDataDocuments(day), testDataBulkBatchSize)
		if err != nil {
			log.Printf("seed test bulk ingest failed for day %s: %v", day, err)
			return
		}
		if _, err := s.etcd.Put(ctx, markerKey, time.Now().UTC().Format(time.RFC3339)); err != nil {
			log.Printf("mark test data loaded failed for day %s: %v", day, err)
			return
		}
		seeded += indexed
	}
	log.Printf("test data ready index=%s from=%s to=%s seeded=%d total=%d", indexName, days[0], days[len(days)-1], seeded, testDataTotalEvents)
}

func (s *Server) postDocumentsInBatches(ctx context.Context, url string, docs []Document, batchSize int) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	if batchSize < 1 {
		batchSize = len(docs)
	}

	indexed := 0
	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		var resp struct {
			OK      bool     `json:"ok"`
			Indexed int      `json:"indexed"`
			Failed  int      `json:"failed"`
			Errors  []string `json:"errors"`
		}
		if err := s.postNDJSONWithTimeout(ctx, url, docs[start:end], testDataBulkTimeout, &resp); err != nil {
			return indexed, err
		}
		if resp.Failed > 0 {
			return indexed, fmt.Errorf("batch %d-%d partially failed: indexed=%d failed=%d errors=%v", start, end-1, resp.Indexed, resp.Failed, resp.Errors)
		}
		indexed += resp.Indexed
	}
	return indexed, nil
}

func testDataDays(reference time.Time) []string {
	base := reference.UTC().Truncate(24 * time.Hour)
	days := make([]string, 0, testDataDayCount)
	for offset := testDataDayCount - 1; offset >= 0; offset-- {
		days = append(days, base.AddDate(0, 0, -offset).Format("2006-01-02"))
	}
	return days
}

func testDataDocuments(day string) []Document {
	services := []string{"api", "ingest", "membership", "storage", "frontend", "scheduler", "billing", "worker"}
	levels := []string{"info", "warn", "error"}
	environments := []string{"prod", "stage", "dev"}
	regions := []string{"eu-west", "us-east", "ap-south"}
	issues := []struct {
		title   string
		message string
		tag     string
	}{
		{title: "API timeout", message: "timeout talking to etcd during bootstrap", tag: "timeouts"},
		{title: "Indexer recovered", message: "replica repair completed for shard sync", tag: "repair"},
		{title: "Search latency spike", message: "query latency exceeded service threshold", tag: "latency"},
		{title: "Node joined cluster", message: "new replica node registered with etcd lease", tag: "cluster"},
		{title: "Disk pressure", message: "bleve segment compaction delayed due to disk pressure", tag: "storage"},
		{title: "Customer search error", message: "customer search request returned partial shard failures", tag: "errors"},
		{title: "Shard rebalanced", message: "primary ownership moved after membership change", tag: "routing"},
		{title: "Worker backlog", message: "background processing queue depth crossed warning threshold", tag: "backlog"},
	}

	base, err := time.Parse("2006-01-02", day)
	if err != nil {
		base = time.Now().UTC().Truncate(24 * time.Hour)
	}
	base = base.UTC()

	docs := make([]Document, 0, testDataEventsPerDay)
	for i := 0; i < testDataEventsPerDay; i++ {
		issue := issues[i%len(issues)]
		service := services[i%len(services)]
		level := levels[(i/3)%len(levels)]
		environment := environments[(i/7)%len(environments)]
		region := regions[(i/11)%len(regions)]
		timestamp := base.Add(time.Duration((i*7)%86400) * time.Second).Format(time.RFC3339)

		docs = append(docs, Document{
			"id":        fmt.Sprintf("evt-%05d", i+1),
			"timestamp": timestamp,
			"title":     issue.title,
			"service":   service,
			"level":     level,
			"message":   fmt.Sprintf("%s on %s in %s", issue.message, service, region),
			"tags": []string{
				environment,
				service,
				issue.tag,
				level,
			},
			"count": 1 + (i % 9),
			"score": 55 + (i % 45),
		})
	}
	return docs
}
