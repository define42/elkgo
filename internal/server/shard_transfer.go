package server

import (
	"context"
	"fmt"
	"strings"
)

func normalizeStreamedBatchItem(route RoutingEntry, doc Document) (internalIndexBatchItem, error) {
	docCopy := cloneDocument(doc)
	rawDocID, ok := docCopy["id"]
	if !ok {
		return internalIndexBatchItem{}, fmt.Errorf("document missing id")
	}

	docID := strings.TrimSpace(fmt.Sprint(rawDocID))
	if docID == "" || docID == "<nil>" {
		return internalIndexBatchItem{}, fmt.Errorf("document missing id")
	}
	docCopy["id"] = docID
	if _, ok := docCopy["partition_day"]; !ok {
		docCopy["partition_day"] = route.Day
	}

	return internalIndexBatchItem{
		DocID: docID,
		Doc:   docCopy,
	}, nil
}

func (s *Server) streamExistingShardDocuments(indexName, day string, shardID int, onDoc func(Document) error) error {
	idx, err := s.openExistingShardIndex(indexName, day, shardID)
	if err != nil {
		return err
	}
	return streamAllDocs(idx, onDoc)
}

func (s *Server) restoreStreamedShardDocuments(route RoutingEntry, stream func(func(Document) error) error) (int, error) {
	items := make([]internalIndexBatchItem, 0, shardSyncBatchSize)
	restored := 0

	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		if err := s.indexBatchLocal(route.IndexName, route.Day, route.ShardID, items); err != nil {
			return err
		}
		restored += len(items)
		items = items[:0]
		return nil
	}

	if err := stream(func(doc Document) error {
		item, err := normalizeStreamedBatchItem(route, doc)
		if err != nil {
			return err
		}
		items = append(items, item)
		if len(items) < shardSyncBatchSize {
			return nil
		}
		return flush()
	}); err != nil {
		return restored, err
	}

	if err := flush(); err != nil {
		return restored, err
	}
	return restored, nil
}

func (s *Server) streamShardToReplica(route RoutingEntry, nodeID string) (int, error) {
	addr, ok := s.memberAddr(nodeID)
	if !ok {
		return 0, fmt.Errorf("replica not registered")
	}

	items := make([]internalIndexBatchItem, 0, shardSyncBatchSize)
	sent := 0

	flush := func() error {
		if len(items) == 0 {
			return nil
		}

		var resp internalIndexBatchResponse
		ctx, cancel := context.WithTimeout(s.backgroundCtx, shardSyncTimeout)
		err := s.postJSON(ctx, addr+"/internal/index_batch", internalIndexBatchRequest{
			IndexName: route.IndexName,
			Day:       route.Day,
			ShardID:   route.ShardID,
			Items:     items,
			Replicate: false,
		}, &resp)
		cancel()
		if err != nil {
			return err
		}
		if !resp.OK {
			return fmt.Errorf("replica repair batch rejected for %s/%s shard %d replica %s", route.IndexName, route.Day, route.ShardID, nodeID)
		}

		sent += len(items)
		items = items[:0]
		return nil
	}

	if err := s.streamExistingShardDocuments(route.IndexName, route.Day, route.ShardID, func(doc Document) error {
		item, err := normalizeStreamedBatchItem(route, doc)
		if err != nil {
			return err
		}
		items = append(items, item)
		if len(items) < shardSyncBatchSize {
			return nil
		}
		return flush()
	}); err != nil {
		return sent, err
	}

	if err := flush(); err != nil {
		return sent, err
	}
	return sent, nil
}
