package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	indexName := strings.TrimSpace(r.URL.Query().Get("index"))
	if indexName == "" {
		http.Error(w, "missing index", http.StatusBadRequest)
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	k := 10
	if raw := r.URL.Query().Get("k"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 1000 {
			k = parsed
		}
	}
	days, err := resolveSearchDays(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	routes := s.snapshotRouting()
	targets := collectTargets(routes, indexName, days)
	if len(targets) == 0 {
		http.Error(w, "routing not initialized for requested index/day range", http.StatusServiceUnavailable)
		return
	}
	type result struct {
		Hits []ShardHit
		Err  error
	}
	ch := make(chan result, len(targets))
	for _, target := range targets {
		go func(target RoutingEntry) {
			replicaNodeID, err := s.pickHealthyReplica(r.Context(), target.IndexName, target.Day, target.ShardID)
			if err != nil {
				ch <- result{Err: err}
				return
			}
			addr, ok := s.memberAddr(replicaNodeID)
			if !ok {
				ch <- result{Err: fmt.Errorf("replica %s has no address", replicaNodeID)}
				return
			}
			var resp SearchShardResponse
			err = s.postJSON(r.Context(), addr+"/internal/search_shard", SearchShardRequest{IndexName: target.IndexName, Day: target.Day, ShardID: target.ShardID, Query: q, K: k * 2}, &resp)
			ch <- result{Hits: resp.Hits, Err: err}
		}(target)
	}
	allHits := make([]ShardHit, 0, len(targets)*k)
	var partial []string
	for i := 0; i < len(targets); i++ {
		res := <-ch
		if res.Err != nil {
			partial = append(partial, res.Err.Error())
			continue
		}
		allHits = append(allHits, res.Hits...)
	}
	sort.Slice(allHits, func(i, j int) bool {
		if allHits[i].Score == allHits[j].Score {
			return allHits[i].DocID < allHits[j].DocID
		}
		return allHits[i].Score > allHits[j].Score
	})
	if len(allHits) > k {
		allHits = allHits[:k]
	}
	writeJSON(w, http.StatusOK, map[string]any{"index": indexName, "days": days, "query": q, "k": k, "hits": allHits, "partial_errors": partial, "shards_per_day": enforcedShardsPerDay})
}

func (s *Server) handleSearchShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SearchShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idx, err := s.openShardIndex(req.IndexName, req.Day, req.ShardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	searchReq := bleve.NewSearchRequestOptions(buildBleveQuery(req.Query), req.K, 0, false)
	searchReq.Fields = []string{"*"}
	searchResult, err := idx.Search(searchReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := SearchShardResponse{Hits: make([]ShardHit, 0, len(searchResult.Hits))}
	for _, h := range searchResult.Hits {
		resp.Hits = append(resp.Hits, ShardHit{Index: req.IndexName, Day: req.Day, Shard: req.ShardID, Score: h.Score, DocID: h.ID, Source: docFromBleveFields(h.Fields)})
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildBleveQuery(q string) query.Query {
	q = strings.TrimSpace(q)
	if q == "" {
		return bleve.NewMatchAllQuery()
	}
	return bleve.NewQueryStringQuery(q)
}

func resolveSearchDays(r *http.Request) ([]string, error) {
	if day := strings.TrimSpace(r.URL.Query().Get("day")); day != "" {
		if _, err := time.Parse("2006-01-02", day); err != nil {
			return nil, errors.New("invalid day (YYYY-MM-DD)")
		}
		return []string{day}, nil
	}
	from := strings.TrimSpace(r.URL.Query().Get("day_from"))
	to := strings.TrimSpace(r.URL.Query().Get("day_to"))
	if from == "" || to == "" {
		return nil, errors.New("provide either day or both day_from and day_to")
	}
	start, err := time.Parse("2006-01-02", from)
	if err != nil {
		return nil, errors.New("invalid day_from")
	}
	end, err := time.Parse("2006-01-02", to)
	if err != nil {
		return nil, errors.New("invalid day_to")
	}
	if end.Before(start) {
		return nil, errors.New("day_to must be >= day_from")
	}
	var days []string
	for d := start; !d.After(end); d = d.Add(24 * time.Hour) {
		days = append(days, d.Format("2006-01-02"))
	}
	return days, nil
}

func collectTargets(routes map[string]RoutingEntry, indexName string, days []string) []RoutingEntry {
	daySet := map[string]struct{}{}
	for _, d := range days {
		daySet[d] = struct{}{}
	}
	out := make([]RoutingEntry, 0)
	for _, rt := range routes {
		if rt.IndexName != indexName {
			continue
		}
		if _, ok := daySet[rt.Day]; !ok {
			continue
		}
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Day == out[j].Day {
			return out[i].ShardID < out[j].ShardID
		}
		return out[i].Day < out[j].Day
	})
	return out
}
