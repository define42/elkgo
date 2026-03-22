package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	blevemapping "github.com/blevesearch/bleve/v2/mapping"
)

var errShardIndexMissing = errors.New("shard index missing")

func streamAllDocs(idx bleve.Index, fn func(Document) error) error {
	const pageSize = 500

	var after []string
	for {
		req := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), pageSize, 0, false)
		req.Fields = []string{"*"}
		req.SortBy([]string{"_id"})
		if len(after) > 0 {
			req.SetSearchAfter(after)
		}

		res, err := idx.Search(req)
		if err != nil {
			return err
		}
		if len(res.Hits) == 0 {
			break
		}

		for _, h := range res.Hits {
			doc := docFromBleveFields(h.Fields)
			if _, ok := doc["id"]; !ok {
				doc["id"] = h.ID
			}
			if err := fn(doc); err != nil {
				return err
			}
		}

		after = res.Hits[len(res.Hits)-1].Sort
	}

	return nil
}

func (s *Server) dumpAllDocs(idx bleve.Index) ([]Document, error) {
	const pageSize = 500

	out := make([]Document, 0, pageSize)
	if err := streamAllDocs(idx, func(doc Document) error {
		out = append(out, cloneDocument(doc))
		return nil
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func shardEventCount(idx bleve.Index) (uint64, error) {
	return idx.DocCount()
}

func pathSizeBytes(path string) (uint64, error) {
	var total uint64

	err := filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, errShardIndexMissing
		}
		return 0, err
	}

	return total, nil
}

func (s *Server) shardStorageSize(indexName, day string, shardID int) (uint64, error) {
	return pathSizeBytes(s.shardIndexPath(indexName, day, shardID))
}

func docFromBleveFields(fields map[string]interface{}) Document {
	doc := Document{}
	for k, v := range fields {
		doc[k] = normalizeBleveField(v)
	}
	return doc
}

func normalizeBleveField(v interface{}) interface{} {
	switch x := v.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(x))
		for _, e := range x {
			out = append(out, normalizeBleveField(e))
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[k] = normalizeBleveField(e)
		}
		return out
	default:
		return x
	}
}

func (s *Server) shardIndexPath(indexName, day string, shardID int) string {
	return filepath.Join(s.dataDir, s.nodeID, indexName, day, fmt.Sprintf("shard-%02d.bleve", shardID))
}

func (s *Server) shardDayPath(indexName, day string) string {
	return filepath.Join(s.dataDir, s.nodeID, indexName, day)
}

func (s *Server) localShardExists(indexName, day string, shardID int) bool {
	cacheKey := partitionKey(indexName, day, shardID)
	s.mu.RLock()
	_, ok := s.indexes[cacheKey]
	s.mu.RUnlock()
	if ok {
		return true
	}
	_, err := os.Stat(s.shardIndexPath(indexName, day, shardID))
	return err == nil
}

func (s *Server) openShardIndex(indexName, day string, shardID int) (bleve.Index, error) {
	return s.openShardIndexWithMode(indexName, day, shardID, true)
}

func (s *Server) openExistingShardIndex(indexName, day string, shardID int) (bleve.Index, error) {
	return s.openShardIndexWithMode(indexName, day, shardID, false)
}

func (s *Server) openShardIndexWithMode(indexName, day string, shardID int, createIfMissing bool) (bleve.Index, error) {
	cacheKey := partitionKey(indexName, day, shardID)
	s.mu.RLock()
	idx, ok := s.indexes[cacheKey]
	s.mu.RUnlock()
	if ok {
		return idx, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.indexes[cacheKey]; ok {
		return idx, nil
	}
	path := s.shardIndexPath(indexName, day, shardID)
	var err error
	if _, statErr := os.Stat(path); statErr == nil {
		idx, err = bleve.Open(path)
	} else {
		if !createIfMissing {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil, errShardIndexMissing
			}
			return nil, statErr
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		idx, err = bleve.New(path, buildIndexMapping())
	}
	if err != nil {
		return nil, err
	}
	s.indexes[cacheKey] = idx
	return idx, nil
}

func (s *Server) removeLocalShardDay(indexName, day string) error {
	prefix := indexName + "|" + day + "|"

	s.mu.Lock()
	for key, idx := range s.indexes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		_ = idx.Close()
		delete(s.indexes, key)
	}
	s.mu.Unlock()

	dayPath := s.shardDayPath(indexName, day)
	if err := os.RemoveAll(dayPath); err != nil {
		return err
	}

	indexPath := filepath.Dir(dayPath)
	if entries, err := os.ReadDir(indexPath); err == nil && len(entries) == 0 {
		if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func buildIndexMapping() blevemapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultAnalyzer = "standard"
	docMapping := bleve.NewDocumentMapping()
	docMapping.Dynamic = true
	keywordField := bleve.NewKeywordFieldMapping()
	docMapping.AddFieldMappingsAt("id", keywordField)
	docMapping.AddFieldMappingsAt("partition_day", keywordField)
	indexMapping.DefaultMapping = docMapping
	return indexMapping
}

func normalizeGenericDocument(doc Document) (string, string, error) {
	if doc == nil {
		return "", "", errors.New("document is required")
	}
	id, ok := asString(doc["id"])
	if !ok || strings.TrimSpace(id) == "" {
		return "", "", errors.New("document must contain a non-empty string field: id")
	}
	doc["id"] = id
	day, err := extractEventDay(doc)
	if err != nil {
		return "", "", err
	}
	doc["partition_day"] = day
	return id, day, nil
}

func extractEventDay(doc Document) (string, error) {
	candidates := []string{"timestamp", "event_time", "created", "ts", "@timestamp"}
	for _, key := range candidates {
		if raw, ok := doc[key]; ok {
			parsed, err := parseTimeValue(raw)
			if err == nil {
				return parsed.UTC().Format("2006-01-02"), nil
			}
		}
	}
	return "", errors.New("document must contain a parseable timestamp field: timestamp, event_time, created, ts, or @timestamp")
}

func parseTimeValue(v interface{}) (time.Time, error) {
	s := fmt.Sprint(v)
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time value: %v", v)
}

func asString(v interface{}) (string, bool) {
	s, ok := v.(string)
	return s, ok
}
