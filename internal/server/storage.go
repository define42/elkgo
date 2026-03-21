package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	blevemapping "github.com/blevesearch/bleve/v2/mapping"
)

func (s *Server) dumpAllDocs(idx bleve.Index) ([]Document, error) {
	req := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), 10000, 0, false)
	req.Fields = []string{"*"}
	res, err := idx.Search(req)
	if err != nil {
		return nil, err
	}
	out := make([]Document, 0, len(res.Hits))
	for _, h := range res.Hits {
		doc := docFromBleveFields(h.Fields)
		if _, ok := doc["id"]; !ok {
			doc["id"] = h.ID
		}
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"]) })
	return out, nil
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

func (s *Server) openShardIndex(indexName, day string, shardID int) (bleve.Index, error) {
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
	path := filepath.Join(s.dataDir, s.nodeID, indexName, day, fmt.Sprintf("shard-%02d.bleve", shardID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	var err error
	if _, statErr := os.Stat(path); statErr == nil {
		idx, err = bleve.Open(path)
	} else {
		idx, err = bleve.New(path, buildIndexMapping())
	}
	if err != nil {
		return nil, err
	}
	s.indexes[cacheKey] = idx
	return idx, nil
}

func buildIndexMapping() blevemapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultAnalyzer = "standard"
	docMapping := bleve.NewDocumentMapping()
	textField := bleve.NewTextFieldMapping()
	textField.Store = true
	textField.Index = true
	textField.IncludeInAll = true
	keywordField := bleve.NewTextFieldMapping()
	keywordField.Store = true
	keywordField.Index = true
	keywordField.Analyzer = "keyword"
	numField := bleve.NewNumericFieldMapping()
	numField.Store = true
	numField.Index = true
	dateField := bleve.NewDateTimeFieldMapping()
	dateField.Store = true
	dateField.Index = true
	docMapping.Dynamic = true
	docMapping.AddFieldMappingsAt("id", keywordField)
	docMapping.AddFieldMappingsAt("title", textField)
	docMapping.AddFieldMappingsAt("body", textField)
	docMapping.AddFieldMappingsAt("message", textField)
	docMapping.AddFieldMappingsAt("tags", textField)
	docMapping.AddFieldMappingsAt("timestamp", dateField)
	docMapping.AddFieldMappingsAt("created", dateField)
	docMapping.AddFieldMappingsAt("event_time", dateField)
	docMapping.AddFieldMappingsAt("partition_day", keywordField)
	docMapping.AddFieldMappingsAt("count", numField)
	docMapping.AddFieldMappingsAt("score", numField)
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
	if title, ok := doc["title"]; ok {
		doc["title"] = fmt.Sprint(title)
	}
	if body, ok := doc["body"]; ok {
		doc["body"] = fmt.Sprint(body)
	}
	if msg, ok := doc["message"]; ok {
		doc["message"] = fmt.Sprint(msg)
	}
	if tags, ok := doc["tags"]; ok {
		doc["tags"] = normalizeStringArray(tags)
	}
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

func normalizeStringArray(v interface{}) []string {
	switch x := v.(type) {
	case []string:
		return append([]string(nil), x...)
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, fmt.Sprint(e))
		}
		return out
	case string:
		return []string{x}
	default:
		return []string{fmt.Sprint(v)}
	}
}
