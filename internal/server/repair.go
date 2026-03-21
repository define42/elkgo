package server

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

func (s *Server) repairLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.repairOnce(ctx)
		}
	}
}

func (s *Server) repairOnce(ctx context.Context) {
	if !s.isCoordinatorMode() {
		return
	}
	routes := s.snapshotRouting()
	for _, rt := range routes {
		if len(rt.Replicas) == 0 || rt.Replicas[0] != s.nodeID {
			continue
		}
		leaderIdx, err := s.openShardIndex(rt.IndexName, rt.Day, rt.ShardID)
		if err != nil {
			log.Printf("repair open leader %s/%s shard %d: %v", rt.IndexName, rt.Day, rt.ShardID, err)
			continue
		}
		leaderDocs, err := s.dumpAllDocs(leaderIdx)
		if err != nil {
			log.Printf("repair dump leader %s/%s shard %d: %v", rt.IndexName, rt.Day, rt.ShardID, err)
			continue
		}
		for _, replica := range rt.Replicas[1:] {
			addr, ok := s.memberAddr(replica)
			if !ok {
				continue
			}
			if err := s.repairReplica(ctx, rt.IndexName, rt.Day, rt.ShardID, addr, leaderDocs); err != nil {
				log.Printf("repair %s/%s shard %d replica %s failed: %v", rt.IndexName, rt.Day, rt.ShardID, replica, err)
			}
		}
	}
}

func (s *Server) repairReplica(ctx context.Context, indexName, day string, shardID int, replicaAddr string, leaderDocs []Document) error {
	var dump DumpDocsResponse
	if err := s.getJSON(ctx, replicaAddr+"/internal/dump_docs?index="+indexName+"&day="+day+"&shard="+strconv.Itoa(shardID), &dump); err != nil {
		return err
	}
	have := make(map[string]struct{}, len(dump.Docs))
	for _, doc := range dump.Docs {
		have[fmt.Sprint(doc["id"])] = struct{}{}
	}
	for _, doc := range leaderDocs {
		docID := fmt.Sprint(doc["id"])
		if _, ok := have[docID]; ok {
			continue
		}
		if err := s.postJSON(ctx, replicaAddr+"/internal/index", internalIndexRequest{IndexName: indexName, Day: day, ShardID: shardID, DocID: docID, Doc: doc}, nil); err != nil {
			return err
		}
	}
	return nil
}
