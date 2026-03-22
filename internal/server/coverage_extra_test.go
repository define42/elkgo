package server

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
)

func TestTransportHelpers_WithRealHTTP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/json-ok":
			var body map[string]any
			if err := decodeJSONRequest(r, &body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"echo":   body["message"],
				"method": r.Method,
			})
		case "/json-empty":
			var body map[string]any
			if err := decodeJSONRequest(r, &body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/json-status-error":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"ok":false,"errors":["replica syncing"]}`)
		case "/json-status-text":
			http.Error(w, "bad gateway", http.StatusBadGateway)
		case "/json-invalid-success":
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `not-json`)
		case "/ndjson-ok":
			reader, err := requestBodyReader(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer reader.Close()

			dec := json.NewDecoder(reader)
			count := 0
			for {
				var doc Document
				if err := dec.Decode(&doc); err != nil {
					if err == io.EOF {
						break
					}
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				count++
			}
			writeJSON(w, http.StatusOK, map[string]any{"count": count})
		case "/ndjson-fail":
			http.Error(w, "upstream failed", http.StatusBadGateway)
		case "/stream-docs":
			enc := json.NewEncoder(w)
			_ = enc.Encode(Document{"id": "doc-1", "message": "alpha"})
			_ = enc.Encode(Document{"id": "doc-2", "message": "beta"})
		case "/stream-status-fail":
			http.Error(w, "not available", http.StatusServiceUnavailable)
		case "/get-json":
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "node": "n1"})
		case "/get-json-fail":
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	s := &Server{client: ts.Client()}
	ctx := context.Background()

	var postResp map[string]any
	if err := s.postJSON(ctx, ts.URL+"/json-ok", map[string]any{"message": "hello"}, &postResp); err != nil {
		t.Fatalf("postJSON success failed: %v", err)
	}
	if postResp["echo"] != "hello" || postResp["method"] != http.MethodPost {
		t.Fatalf("unexpected postJSON response: %#v", postResp)
	}
	if err := s.postJSON(ctx, ts.URL+"/json-empty", map[string]any{"message": "discard"}, nil); err != nil {
		t.Fatalf("postJSON nil-out failed: %v", err)
	}
	if err := s.postJSON(ctx, ts.URL+"/json-status-text", map[string]any{"message": "fail"}, nil); err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected postJSON status error, got %v", err)
	}

	var statusResp internalIndexBatchResponse
	status, err := s.postJSONStatus(ctx, ts.URL+"/json-status-error", map[string]any{"message": "retry"}, &statusResp)
	if err != nil {
		t.Fatalf("postJSONStatus JSON error payload failed: %v", err)
	}
	if status != http.StatusServiceUnavailable || len(statusResp.Errors) != 1 || statusResp.Errors[0] != "replica syncing" {
		t.Fatalf("unexpected postJSONStatus response: status=%d payload=%#v", status, statusResp)
	}
	status, err = s.postJSONStatus(ctx, ts.URL+"/json-status-text", map[string]any{"message": "retry"}, nil)
	if err == nil || status != http.StatusBadGateway || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected postJSONStatus text error, got status=%d err=%v", status, err)
	}
	status, err = s.postJSONStatus(ctx, ts.URL+"/json-invalid-success", map[string]any{"message": "retry"}, &map[string]any{})
	if err == nil || status != http.StatusOK {
		t.Fatalf("expected postJSONStatus invalid success body error, got status=%d err=%v", status, err)
	}

	var ndjsonResp map[string]int
	if err := postNDJSONWithClient(ctx, ts.Client(), ts.URL+"/ndjson-ok", []Document{
		{"id": "doc-1"},
		{"id": "doc-2"},
	}, &ndjsonResp); err != nil {
		t.Fatalf("postNDJSONWithClient success failed: %v", err)
	}
	if ndjsonResp["count"] != 2 {
		t.Fatalf("unexpected NDJSON count: %#v", ndjsonResp)
	}
	if err := postNDJSONWithClient(ctx, ts.Client(), ts.URL+"/ndjson-fail", []Document{{"id": "doc-1"}}, nil); err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected NDJSON status error, got %v", err)
	}

	var streamed []Document
	if err := streamDocumentsWithClient(ctx, ts.Client(), ts.URL+"/stream-docs", func(doc Document) error {
		streamed = append(streamed, doc)
		return nil
	}); err != nil {
		t.Fatalf("streamDocumentsWithClient success failed: %v", err)
	}
	if len(streamed) != 2 || streamed[1]["id"] != "doc-2" {
		t.Fatalf("unexpected streamed documents: %#v", streamed)
	}
	if err := streamDocumentsWithClient(ctx, ts.Client(), ts.URL+"/stream-docs", func(doc Document) error {
		return errors.New("stop here")
	}); err == nil || !strings.Contains(err.Error(), "stop here") {
		t.Fatalf("expected callback error from streamDocumentsWithClient, got %v", err)
	}
	if err := streamDocumentsWithClient(ctx, ts.Client(), ts.URL+"/stream-status-fail", func(doc Document) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("expected streamDocumentsWithClient status error, got %v", err)
	}

	var getResp map[string]any
	if err := getJSONWithClient(ctx, ts.Client(), ts.URL+"/get-json", &getResp); err != nil {
		t.Fatalf("getJSONWithClient success failed: %v", err)
	}
	if getResp["node"] != "n1" || getResp["ok"] != true {
		t.Fatalf("unexpected getJSONWithClient payload: %#v", getResp)
	}
	if err := getJSONWithClient(ctx, ts.Client(), ts.URL+"/get-json-fail", &getResp); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected getJSONWithClient status error, got %v", err)
	}

	jsonReq, err := newStreamingJSONRequest(ctx, http.MethodPut, "http://example.invalid/json", map[string]any{"message": "plain"}, false)
	if err != nil {
		t.Fatalf("newStreamingJSONRequest: %v", err)
	}
	reader, err := requestBodyReader(jsonReq)
	if err != nil {
		t.Fatalf("requestBodyReader for plain JSON: %v", err)
	}
	var plainBody map[string]any
	if err := json.NewDecoder(reader).Decode(&plainBody); err != nil {
		reader.Close()
		t.Fatalf("decode plain request body: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close plain request body: %v", err)
	}
	if plainBody["message"] != "plain" {
		t.Fatalf("unexpected plain request body: %#v", plainBody)
	}

	ndjsonReq, err := newStreamingNDJSONRequest(ctx, "http://example.invalid/ndjson", []Document{{"id": "a"}, {"id": "b"}}, false)
	if err != nil {
		t.Fatalf("newStreamingNDJSONRequest: %v", err)
	}
	ndjsonReader, err := requestBodyReader(ndjsonReq)
	if err != nil {
		t.Fatalf("requestBodyReader for plain NDJSON: %v", err)
	}
	ndjsonDec := json.NewDecoder(ndjsonReader)
	count := 0
	for {
		var doc Document
		if err := ndjsonDec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			ndjsonReader.Close()
			t.Fatalf("decode plain NDJSON request body: %v", err)
		}
		count++
	}
	if err := ndjsonReader.Close(); err != nil {
		t.Fatalf("close plain NDJSON body: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 NDJSON docs, got %d", count)
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-gzip"))
	req.Header.Set("Content-Encoding", "gzip")
	if _, err := requestBodyReader(req); err == nil {
		t.Fatalf("expected requestBodyReader to reject invalid gzip body")
	}

	tmp, err := os.CreateTemp(t.TempDir(), "close-twice-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	closer := &compositeReadCloser{
		Reader: strings.NewReader(""),
		closers: []io.Closer{
			tmp,
			tmp,
		},
	}
	if err := closer.Close(); err == nil {
		t.Fatalf("expected compositeReadCloser to report the second close error")
	}
}

func TestSnapshotHelpers_WithRealFiles(t *testing.T) {
	t.Run("filesystem writer validates paths", func(t *testing.T) {
		baseDir := t.TempDir()
		writer := &filesystemDirectoryWriter{baseDir: baseDir}

		if _, err := writer.GetWriter(""); err == nil {
			t.Fatalf("expected empty snapshot path to fail")
		}
		if _, err := writer.GetWriter("../escape.txt"); err == nil {
			t.Fatalf("expected path traversal to fail")
		}

		fileWriter, err := writer.GetWriter("nested/file.txt")
		if err != nil {
			t.Fatalf("GetWriter valid path failed: %v", err)
		}
		if _, err := io.WriteString(fileWriter, "snapshot-data"); err != nil {
			fileWriter.Close()
			t.Fatalf("write nested file: %v", err)
		}
		if err := fileWriter.Close(); err != nil {
			t.Fatalf("close nested file: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(baseDir, "nested", "file.txt"))
		if err != nil {
			t.Fatalf("read nested file: %v", err)
		}
		if string(got) != "snapshot-data" {
			t.Fatalf("unexpected nested file contents: %q", string(got))
		}
	})

	t.Run("copy zip extract and download", func(t *testing.T) {
		srcDir := filepath.Join(t.TempDir(), "src")
		if err := os.MkdirAll(filepath.Join(srcDir, "nested"), 0o755); err != nil {
			t.Fatalf("MkdirAll src: %v", err)
		}
		if err := os.WriteFile(filepath.Join(srcDir, "root.txt"), []byte("root"), 0o644); err != nil {
			t.Fatalf("WriteFile root: %v", err)
		}
		if err := os.WriteFile(filepath.Join(srcDir, "nested", "child.txt"), []byte("child"), 0o644); err != nil {
			t.Fatalf("WriteFile child: %v", err)
		}

		copyPath := filepath.Join(t.TempDir(), "copied", "child.txt")
		if err := copyFile(filepath.Join(srcDir, "nested", "child.txt"), copyPath); err != nil {
			t.Fatalf("copyFile failed: %v", err)
		}
		if got, err := os.ReadFile(copyPath); err != nil || string(got) != "child" {
			t.Fatalf("unexpected copied file contents: %q err=%v", string(got), err)
		}

		archivePath := filepath.Join(t.TempDir(), "snapshot.zip")
		if err := zipDirectory(srcDir, archivePath); err != nil {
			t.Fatalf("zipDirectory failed: %v", err)
		}
		extractDir := filepath.Join(t.TempDir(), "extracted")
		if err := extractSnapshotArchive(archivePath, extractDir); err != nil {
			t.Fatalf("extractSnapshotArchive failed: %v", err)
		}
		if got, err := os.ReadFile(filepath.Join(extractDir, "nested", "child.txt")); err != nil || string(got) != "child" {
			t.Fatalf("unexpected extracted child contents: %q err=%v", string(got), err)
		}

		downloadTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ok":
				http.ServeFile(w, r, archivePath)
			default:
				http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
			}
		}))
		defer downloadTS.Close()

		destPath := filepath.Join(t.TempDir(), "downloaded.zip")
		if err := downloadSnapshotArchive(context.Background(), downloadTS.Client(), downloadTS.URL+"/ok", time.Second, destPath); err != nil {
			t.Fatalf("downloadSnapshotArchive success failed: %v", err)
		}
		if _, err := os.Stat(destPath); err != nil {
			t.Fatalf("downloaded archive missing: %v", err)
		}
		if err := downloadSnapshotArchive(context.Background(), downloadTS.Client(), downloadTS.URL+"/fail", time.Second, filepath.Join(t.TempDir(), "missing.zip")); err == nil || !strings.Contains(err.Error(), "status 503") {
			t.Fatalf("expected downloadSnapshotArchive status error, got %v", err)
		}

		badArchive := filepath.Join(t.TempDir(), "bad.zip")
		file, err := os.Create(badArchive)
		if err != nil {
			t.Fatalf("create bad archive: %v", err)
		}
		zipWriter := zip.NewWriter(file)
		entry, err := zipWriter.Create("../escape.txt")
		if err != nil {
			t.Fatalf("create bad archive entry: %v", err)
		}
		if _, err := io.WriteString(entry, "escape"); err != nil {
			t.Fatalf("write bad archive entry: %v", err)
		}
		if err := zipWriter.Close(); err != nil {
			t.Fatalf("close bad archive zip writer: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close bad archive file: %v", err)
		}
		if err := extractSnapshotArchive(badArchive, filepath.Join(t.TempDir(), "bad-extract")); err == nil {
			t.Fatalf("expected extractSnapshotArchive to reject invalid paths")
		}
	})

	t.Run("restore and install snapshot no-op when shard exists", func(t *testing.T) {
		source, sourceTS := newNamedTestHTTPServer(t, "n1")
		target, _ := newNamedTestHTTPServer(t, "n2")

		day := "2026-03-21"
		shardID := 0
		setTestRoute(source, "events", day, shardID, []string{"n1"})
		setTestRoute(target, "events", day, shardID, []string{"n2"})

		indexTestDocument(t, source, "events", day, shardID, "source-doc", Document{
			"id":        "source-doc",
			"timestamp": day + "T10:00:00Z",
			"message":   "source snapshot",
		})
		indexTestDocument(t, target, "events", day, shardID, "target-doc", Document{
			"id":        "target-doc",
			"timestamp": day + "T10:05:00Z",
			"message":   "target existing",
		})

		restored, err := target.restoreShardSnapshotFromURL(context.Background(), RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n2"},
		}, sourceTS.URL+"/internal/snapshot_shard?index=events&day="+day+"&shard=0", time.Second)
		if err != nil {
			t.Fatalf("restoreShardSnapshotFromURL with existing shard failed: %v", err)
		}
		if restored {
			t.Fatalf("expected restoreShardSnapshotFromURL to no-op when shard already exists")
		}

		archivePath := filepath.Join(t.TempDir(), "source.zip")
		idx, err := source.openExistingShardIndex("events", day, shardID)
		if err != nil {
			t.Fatalf("openExistingShardIndex source: %v", err)
		}
		if err := source.writeShardSnapshotArchive("events", day, shardID, idx, archivePath); err != nil {
			t.Fatalf("writeShardSnapshotArchive: %v", err)
		}
		extractedDir := filepath.Join(t.TempDir(), "extracted")
		if err := extractSnapshotArchive(archivePath, extractedDir); err != nil {
			t.Fatalf("extractSnapshotArchive source archive: %v", err)
		}
		if err := target.installExtractedShardSnapshot("events", day, shardID, extractedDir, false); err != nil {
			t.Fatalf("installExtractedShardSnapshot no-replace failed: %v", err)
		}
		docs, err := target.dumpAllDocs("events", day, shardID)
		if err != nil {
			t.Fatalf("dump target docs after no-op install: %v", err)
		}
		if len(docs) != 1 || docs[0]["id"] != "target-doc" {
			t.Fatalf("unexpected docs after no-op install: %#v", docs)
		}
	})
}

func TestSearchHelpers_FailoverAndValidation(t *testing.T) {
	t.Run("replica failover for shard refs and document fetch", func(t *testing.T) {
		coordinator, _ := newNamedTestHTTPServer(t, "n1")
		healthy, healthyTS := newNamedTestHTTPServer(t, "n3")

		day := "2026-03-21"
		shardID := 0
		setTestRoute(healthy, "events", day, shardID, []string{"n3"})
		indexTestDocument(t, healthy, "events", day, shardID, "search-doc", Document{
			"id":        "search-doc",
			"timestamp": day + "T11:00:00Z",
			"message":   "failover search token",
		})

		failingTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer failingTS.Close()

		coordinator.membersMu.Lock()
		coordinator.members = map[string]NodeInfo{
			"n1": {ID: "n1", Addr: "http://127.0.0.1"},
			"n2": {ID: "n2", Addr: failingTS.URL},
			"n3": {ID: "n3", Addr: healthyTS.URL},
		}
		coordinator.membersMu.Unlock()

		target := RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n2", "n3"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}

		refs, err := coordinator.searchShardRefs(context.Background(), target, "failover search", 5)
		if err != nil {
			t.Fatalf("searchShardRefs failover failed: %v", err)
		}
		if len(refs) != 1 || refs[0].DocID != "search-doc" {
			t.Fatalf("unexpected search refs after failover: %#v", refs)
		}

		docs, err := coordinator.fetchDocumentsForShard(context.Background(), target, []string{"search-doc"})
		if err != nil {
			t.Fatalf("fetchDocumentsForShard failover failed: %v", err)
		}
		if len(docs) != 1 || docs["search-doc"]["message"] != "failover search token" {
			t.Fatalf("unexpected fetched docs after failover: %#v", docs)
		}
	})

	t.Run("internal search handlers validate method sync state and missing shard", func(t *testing.T) {
		s, ts := newTestHTTPServer(t)

		day := "2026-03-21"
		shardID := 4
		setTestRoute(s, "events", day, shardID, []string{"n1"})

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/internal/search_shard", nil)
		if err != nil {
			t.Fatalf("new GET search_shard request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET search_shard failed: %v", err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("expected GET search_shard status 405, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		resp, err = http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader("{"))
		if err != nil {
			t.Fatalf("invalid JSON search_shard failed: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected invalid JSON search_shard status 400, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		reqBody := `{"index_name":"events","day":"2026-03-21","shard_id":4,"query":"alpha","k":5}`
		key := routingMapKey("events", day, shardID)
		route, ok := s.getRouting("events", day, shardID)
		if !ok {
			t.Fatalf("expected route to exist")
		}
		s.shardSyncMu.Lock()
		s.shardSyncPending[key] = route.Version
		s.shardSyncMu.Unlock()

		resp, err = http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("replica syncing search_shard failed: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected replica syncing status 503, got %d", resp.StatusCode)
		}
		if !strings.Contains(readAllAndClose(t, resp), "replica syncing") {
			t.Fatalf("expected replica syncing error body")
		}

		s.shardSyncMu.Lock()
		delete(s.shardSyncPending, key)
		s.shardSyncMu.Unlock()

		resp, err = http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("missing shard search_shard failed: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected missing shard status 503, got %d", resp.StatusCode)
		}
		if !strings.Contains(readAllAndClose(t, resp), "shard not available") {
			t.Fatalf("expected shard not available body")
		}

		indexTestDocument(t, s, "events", day, shardID, "doc-a", Document{
			"id":        "doc-a",
			"timestamp": day + "T12:00:00Z",
			"message":   "alpha token",
		})
		resp, err = http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("search_shard success failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected search_shard status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
		}
		var payload SearchShardResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			t.Fatalf("decode search_shard response: %v", err)
		}
		resp.Body.Close()
		if len(payload.Hits) != 1 || payload.Hits[0].Source != nil {
			t.Fatalf("unexpected search_shard payload without fetch_docs: %#v", payload)
		}
	})
}

func TestWALHelpers_ErrorBranchesAndCleanup(t *testing.T) {
	t.Run("int64 conversion covers supported and invalid values", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			value any
			want  int64
			ok    bool
		}{
			{name: "nil", value: nil, ok: false},
			{name: "int", value: int(3), want: 3, ok: true},
			{name: "int8", value: int8(4), want: 4, ok: true},
			{name: "int16", value: int16(5), want: 5, ok: true},
			{name: "int32", value: int32(6), want: 6, ok: true},
			{name: "int64", value: int64(7), want: 7, ok: true},
			{name: "uint", value: uint(8), want: 8, ok: true},
			{name: "uint8", value: uint8(9), want: 9, ok: true},
			{name: "uint16", value: uint16(10), want: 10, ok: true},
			{name: "uint32", value: uint32(11), want: 11, ok: true},
			{name: "uint64", value: uint64(12), want: 12, ok: true},
			{name: "uint64 too large", value: ^uint64(0), ok: false},
			{name: "float32", value: float32(13), want: 13, ok: true},
			{name: "float64", value: float64(14), want: 14, ok: true},
			{name: "json number", value: json.Number("15"), want: 15, ok: true},
			{name: "json number invalid", value: json.Number("bad"), ok: false},
			{name: "string", value: "16", want: 16, ok: true},
			{name: "string invalid", value: "bad", ok: false},
			{name: "default", value: struct{}{}, ok: false},
		} {
			got, ok := int64FromValue(tc.value)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("%s: int64FromValue(%#v) = (%d,%v), want (%d,%v)", tc.name, tc.value, got, ok, tc.want, tc.ok)
			}
		}
	})

	t.Run("source record validation errors are surfaced", func(t *testing.T) {
		raw := []byte(`{"id":"doc-1","message":"wal data"}`)
		compressed := compressSourceRecord(raw)

		baseFile := filepath.Join(t.TempDir(), "record.wal")
		file, err := os.Create(baseFile)
		if err != nil {
			t.Fatalf("Create record file: %v", err)
		}
		if err := writeSourceRecord(file, raw, compressed); err != nil {
			file.Close()
			t.Fatalf("writeSourceRecord: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close record file: %v", err)
		}

		mustRead := func(path string) *os.File {
			t.Helper()
			f, err := os.Open(path)
			if err != nil {
				t.Fatalf("Open %s: %v", path, err)
			}
			return f
		}
		writeBytes := func(path string, data []byte) {
			t.Helper()
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatalf("WriteFile %s: %v", path, err)
			}
		}

		validData, err := os.ReadFile(baseFile)
		if err != nil {
			t.Fatalf("ReadFile base record: %v", err)
		}
		validPointer := sourcePointer{
			Segment:          1,
			Offset:           0,
			CompressedLength: uint64(len(compressed)),
			RawLength:        uint64(len(raw)),
		}

		checkReadErr := func(name string, mutate func([]byte), pointer sourcePointer, wantContains string) {
			t.Helper()
			path := filepath.Join(t.TempDir(), name+".wal")
			data := append([]byte(nil), validData...)
			mutate(data)
			writeBytes(path, data)
			f := mustRead(path)
			defer f.Close()
			if _, err := readSourceRecord(f, pointer); err == nil || !strings.Contains(err.Error(), wantContains) {
				t.Fatalf("%s: expected error containing %q, got %v", name, wantContains, err)
			}
		}

		checkReadErr("bad-magic", func(data []byte) { copy(data[:4], []byte("BORK")) }, validPointer, "invalid source record magic")
		checkReadErr("bad-version", func(data []byte) { data[4] = 9 }, validPointer, "unsupported source record version")
		checkReadErr("bad-codec", func(data []byte) { data[5] = 9 }, validPointer, "unsupported source record codec")
		checkReadErr("bad-checksum", func(data []byte) { data[24]++ }, validPointer, "source checksum mismatch")

		f := mustRead(baseFile)
		defer f.Close()
		if _, err := readSourceRecord(f, sourcePointer{
			Segment:          1,
			Offset:           0,
			CompressedLength: validPointer.CompressedLength,
			RawLength:        validPointer.RawLength + 1,
		}); err == nil || !strings.Contains(err.Error(), "source raw length mismatch") {
			t.Fatalf("expected raw length mismatch, got %v", err)
		}
		if _, err := readSourceRecord(f, sourcePointer{
			Segment:          1,
			Offset:           0,
			CompressedLength: validPointer.CompressedLength + 1,
			RawLength:        validPointer.RawLength,
		}); err == nil || !strings.Contains(err.Error(), "source compressed length mismatch") {
			t.Fatalf("expected compressed length mismatch, got %v", err)
		}
	})

	t.Run("read source document injects missing id and shard cleanup removes wal", func(t *testing.T) {
		s, _ := newTestHTTPServer(t)
		day := "2026-03-21"
		shardID := 0
		setTestRoute(s, "events", day, shardID, []string{"n1"})

		docID, doc, raw, err := materializeBatchItemSource(internalIndexBatchItem{
			Doc: Document{
				"id":        "cleanup-doc",
				"timestamp": day + "T13:00:00Z",
				"message":   "cleanup me",
			},
		}, day)
		if err != nil {
			t.Fatalf("materializeBatchItemSource: %v", err)
		}
		if docID != "cleanup-doc" || doc["message"] != "cleanup me" || len(raw) == 0 {
			t.Fatalf("unexpected materialized batch item: id=%q doc=%#v raw=%q", docID, doc, string(raw))
		}

		indexTestDocument(t, s, "events", day, shardID, "cleanup-doc", Document{
			"id":        "cleanup-doc",
			"timestamp": day + "T13:00:00Z",
			"message":   "cleanup me",
		})

		idx, err := s.openExistingShardIndex("events", day, shardID)
		if err != nil {
			t.Fatalf("openExistingShardIndex: %v", err)
		}
		req := bleve.NewSearchRequestOptions(bleve.NewDocIDQuery([]string{"cleanup-doc"}), 1, 0, false)
		req.Fields = sourcePointerFieldNames()
		res, err := idx.Search(req)
		if err != nil {
			t.Fatalf("search source pointer fields: %v", err)
		}
		if len(res.Hits) != 1 {
			t.Fatalf("expected 1 hit, got %d", len(res.Hits))
		}
		reader := newShardSourceReader(s, "events", day, shardID)
		defer reader.Close()
		doc, err = readSourceDocumentFromFields(reader, "cleanup-doc", map[string]interface{}{
			sourceFieldSegment:          res.Hits[0].Fields[sourceFieldSegment],
			sourceFieldOffset:           res.Hits[0].Fields[sourceFieldOffset],
			sourceFieldCompressedLength: res.Hits[0].Fields[sourceFieldCompressedLength],
			sourceFieldRawLength:        res.Hits[0].Fields[sourceFieldRawLength],
		})
		if err != nil {
			t.Fatalf("readSourceDocumentFromFields: %v", err)
		}
		delete(doc, "id")
		doc, err = readSourceDocumentFromFields(reader, "cleanup-doc", map[string]interface{}{
			sourceFieldSegment:          res.Hits[0].Fields[sourceFieldSegment],
			sourceFieldOffset:           res.Hits[0].Fields[sourceFieldOffset],
			sourceFieldCompressedLength: res.Hits[0].Fields[sourceFieldCompressedLength],
			sourceFieldRawLength:        res.Hits[0].Fields[sourceFieldRawLength],
		})
		if err != nil {
			t.Fatalf("readSourceDocumentFromFields second read: %v", err)
		}
		if doc["id"] != "cleanup-doc" {
			t.Fatalf("expected missing id to be injected, got %#v", doc)
		}

		if err := s.removeLocalShardFiles("events", day, shardID); err != nil {
			t.Fatalf("removeLocalShardFiles: %v", err)
		}
		if s.localShardExists("events", day, shardID) {
			t.Fatalf("expected shard index to be removed")
		}
		paths, err := s.shardSourceSegmentPaths("events", day, shardID)
		if err != nil {
			t.Fatalf("shardSourceSegmentPaths after cleanup: %v", err)
		}
		if len(paths) != 0 {
			t.Fatalf("expected WAL segments to be removed, got %#v", paths)
		}
	})
}

func TestSnapshotAndIngestHelpers_RetryAndReplicaPaths(t *testing.T) {
	t.Run("transfer and restore snapshot through candidates", func(t *testing.T) {
		source, sourceTS := newNamedTestHTTPServer(t, "n1")
		replica, replicaTS := newNamedTestHTTPServer(t, "n2")
		target, _ := newNamedTestHTTPServer(t, "n3")

		day := "2026-03-21"
		shardID := 0
		setTestRoute(source, "events", day, shardID, []string{"n1", "n2"})
		setTestRoute(replica, "events", day, shardID, []string{"n2"})
		setTestRoute(target, "events", day, shardID, []string{"n3"})

		indexTestDocument(t, source, "events", day, shardID, "snap-doc", Document{
			"id":        "snap-doc",
			"timestamp": day + "T14:00:00Z",
			"message":   "candidate snapshot",
		})

		source.membersMu.Lock()
		source.members["n2"] = NodeInfo{ID: "n2", Addr: replicaTS.URL}
		source.membersMu.Unlock()

		if err := source.transferShardSnapshotToReplica(context.Background(), RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n1", "n2"},
		}, "n2"); err != nil {
			t.Fatalf("transferShardSnapshotToReplica: %v", err)
		}
		replicaDocs, err := replica.dumpAllDocs("events", day, shardID)
		if err != nil {
			t.Fatalf("dump replica docs after transfer: %v", err)
		}
		if len(replicaDocs) != 1 || replicaDocs[0]["id"] != "snap-doc" {
			t.Fatalf("unexpected replica docs after transfer: %#v", replicaDocs)
		}

		target.membersMu.Lock()
		target.members["n1"] = NodeInfo{ID: "n1", Addr: sourceTS.URL}
		target.membersMu.Unlock()
		restored, nodeID, err := target.restoreShardSnapshotFromCandidates(context.Background(), shardSyncTask{
			previous: RoutingEntry{IndexName: "events", Day: day, ShardID: shardID, Replicas: []string{"n1", "n2"}},
			current:  RoutingEntry{IndexName: "events", Day: day, ShardID: shardID, Replicas: []string{"n3", "n1"}},
		})
		if err != nil {
			t.Fatalf("restoreShardSnapshotFromCandidates: %v", err)
		}
		if !restored || nodeID != "n1" {
			t.Fatalf("unexpected restore result: restored=%v node=%q", restored, nodeID)
		}
		targetDocs, err := target.dumpAllDocs("events", day, shardID)
		if err != nil {
			t.Fatalf("dump target docs after candidate restore: %v", err)
		}
		if len(targetDocs) != 1 || targetDocs[0]["message"] != "candidate snapshot" {
			t.Fatalf("unexpected target docs after candidate restore: %#v", targetDocs)
		}
	})

	t.Run("bulk group and single document retry remote primary and quorum failure marks repair", func(t *testing.T) {
		coordinator, _ := newNamedTestHTTPServer(t, "n1")
		remote, remoteTS := newNamedTestHTTPServer(t, "n2")

		day := "2026-03-22"
		shardID := 3
		route := RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n2"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		setTestRoute(remote, "events", day, shardID, []string{"n2"})

		go func() {
			time.Sleep(routeRetryDelay + 50*time.Millisecond)
			coordinator.membersMu.Lock()
			coordinator.members["n2"] = NodeInfo{ID: "n2", Addr: remoteTS.URL}
			coordinator.membersMu.Unlock()
		}()

		group := bulkShardGroup{
			indexName: "events",
			day:       day,
			shardID:   shardID,
			route:     route,
			items: []bulkPreparedItem{{
				lineNo:    1,
				indexName: "events",
				day:       day,
				shardID:   shardID,
				route:     route,
				item: internalIndexBatchItem{
					DocID: "bulk-retry-doc",
					Doc: Document{
						"id":        "bulk-retry-doc",
						"timestamp": day + "T09:00:00Z",
						"message":   "bulk retry",
					},
				},
			}},
		}
		if err := coordinator.ingestBulkShardGroup(context.Background(), group); err != nil {
			t.Fatalf("ingestBulkShardGroup retry failed: %v", err)
		}
		docs, err := remote.dumpAllDocs("events", day, shardID)
		if err != nil {
			t.Fatalf("dump remote docs after bulk retry: %v", err)
		}
		if len(docs) != 1 || docs[0]["id"] != "bulk-retry-doc" {
			t.Fatalf("unexpected docs after bulk retry: %#v", docs)
		}

		go func() {
			time.Sleep(routeRetryDelay + 50*time.Millisecond)
			coordinator.membersMu.Lock()
			coordinator.members["n2"] = NodeInfo{ID: "n2", Addr: remoteTS.URL}
			coordinator.membersMu.Unlock()
		}()

		status, resp, err := coordinator.indexSingleDocument(context.Background(), "events", day, shardID, route, "single-retry-doc", Document{
			"id":        "single-retry-doc",
			"timestamp": day + "T09:05:00Z",
			"message":   "single retry",
		})
		if err != nil {
			t.Fatalf("indexSingleDocument retry failed: %v", err)
		}
		if status != http.StatusOK || !resp.OK {
			t.Fatalf("unexpected indexSingleDocument response: status=%d resp=%#v", status, resp)
		}

		failingReplicaTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "replica write failed", http.StatusInternalServerError)
		}))
		defer failingReplicaTS.Close()

		coordinator.membersMu.Lock()
		coordinator.members["n2"] = NodeInfo{ID: "n2", Addr: failingReplicaTS.URL}
		coordinator.membersMu.Unlock()

		localRoute := RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   4,
			Replicas:  []string{"n1", "n2"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		status, resp, err = coordinator.indexBatchOnPrimary(context.Background(), "events", day, 4, localRoute, []internalIndexBatchItem{{
			DocID: "quorum-fail-doc",
			Doc: Document{
				"id":        "quorum-fail-doc",
				"timestamp": day + "T09:10:00Z",
				"message":   "quorum failure",
			},
		}})
		if err != nil {
			t.Fatalf("indexBatchOnPrimary quorum failure returned unexpected error: %v", err)
		}
		if status != http.StatusServiceUnavailable || resp.OK || resp.Acks != 1 || resp.Quorum != 2 {
			t.Fatalf("unexpected quorum failure response: status=%d resp=%#v", status, resp)
		}
		if !coordinator.replicaNeedsRepair("events", day, 4, "n2") {
			t.Fatalf("expected failed replica to be marked for repair")
		}
	})
}

func TestInternalHandlers_ValidationAndStateBranches(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-23"
	shardID := 6
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/internal/index", nil)
	if err != nil {
		t.Fatalf("new GET /internal/index request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET /internal/index status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("invalid JSON /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected invalid JSON /internal/index status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-23","shard_id":9,"doc":{"id":"wrong-shard","timestamp":"2026-03-23T10:00:00Z"}}`))
	if err != nil {
		t.Fatalf("non-replica /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected non-replica /internal/index status 403, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	key := routingMapKey("events", day, shardID)
	route, ok := s.getRouting("events", day, shardID)
	if !ok {
		t.Fatalf("expected route to exist")
	}
	s.shardSyncMu.Lock()
	s.shardSyncPending[key] = route.Version
	s.shardSyncMu.Unlock()

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-23","shard_id":6,"doc":{"id":"syncing-doc","timestamp":"2026-03-23T10:05:00Z"}}`))
	if err != nil {
		t.Fatalf("syncing /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected syncing /internal/index status 503, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	s.shardSyncMu.Lock()
	delete(s.shardSyncPending, key)
	s.shardSyncMu.Unlock()

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-23","shard_id":6,"doc":{"id":"bad-doc"}}`))
	if err != nil {
		t.Fatalf("bad doc /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad doc /internal/index status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-23","shard_id":6,"doc":{"id":"ok-doc","timestamp":"2026-03-23T10:10:00Z","message":"indexed"}}`))
	if err != nil {
		t.Fatalf("successful /internal/index failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected successful /internal/index status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/internal/shard_stats?index=events&day="+day+"&shard=bad", nil)
	if err != nil {
		t.Fatalf("new bad shard stats request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("bad shard stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad shard stats status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/internal/shard_stats?index=events&day=" + day + "&shard=9")
	if err != nil {
		t.Fatalf("forbidden shard stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden shard stats status 403, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	setTestRoute(s, "events", day, 7, []string{"n1"})
	resp, err = http.Get(ts.URL + "/internal/shard_stats?index=events&day=" + day + "&shard=7")
	if err != nil {
		t.Fatalf("missing shard stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected missing shard stats status 503, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	indexTestDocument(t, s, "events", day, shardID, "stats-doc", Document{
		"id":        "stats-doc",
		"timestamp": day + "T10:15:00Z",
		"message":   "stats",
	})
	resp, err = http.Get(ts.URL + "/internal/shard_stats?index=events&day=" + day + "&shard=6")
	if err != nil {
		t.Fatalf("successful shard stats request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected successful shard stats status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodPost, ts.URL+"/internal/snapshot_shard?index=events&day="+day+"&shard=6", nil)
	if err != nil {
		t.Fatalf("new POST /internal/snapshot_shard request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /internal/snapshot_shard failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected POST /internal/snapshot_shard status 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/internal/snapshot_shard?index=events&day=" + day + "&shard=bad")
	if err != nil {
		t.Fatalf("bad snapshot request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad snapshot status 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/internal/snapshot_shard?index=events&day=" + day + "&shard=8")
	if err != nil {
		t.Fatalf("forbidden snapshot request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden snapshot status 403, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	setTestRoute(s, "events", day, 10, []string{"n1"})
	resp, err = http.Get(ts.URL + "/internal/snapshot_shard?index=events&day=" + day + "&shard=10")
	if err != nil {
		t.Fatalf("missing shard snapshot request failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected missing shard snapshot status 503, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/internal/snapshot_shard?index=events&day=" + day + "&shard=6")
	if err != nil {
		t.Fatalf("successful snapshot request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected successful snapshot status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "application/zip") {
		t.Fatalf("unexpected snapshot content type: %q", got)
	}
	resp.Body.Close()
}

func TestStorageAndShardTransferHelpers(t *testing.T) {
	t.Run("storage helpers cover normalization parsing and cleanup", func(t *testing.T) {
		s, _ := newTestHTTPServer(t)
		day := "2026-03-24"
		shardID := 0

		if _, err := s.openExistingShardIndex("events", day, shardID); !errors.Is(err, errShardIndexMissing) {
			t.Fatalf("expected missing shard index error, got %v", err)
		}
		if _, err := pathSizeBytes(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, errShardIndexMissing) {
			t.Fatalf("expected missing path size error, got %v", err)
		}
		if _, _, err := normalizeGenericDocument(nil); err == nil {
			t.Fatalf("expected normalizeGenericDocument to reject nil")
		}
		if _, _, err := normalizeGenericDocument(Document{"timestamp": day + "T00:00:00Z"}); err == nil {
			t.Fatalf("expected normalizeGenericDocument to require id")
		}

		doc := Document{
			"id":         "doc-storage",
			"event_time": day + "T12:00:00Z",
		}
		docID, normalizedDay, err := normalizeGenericDocument(doc)
		if err != nil {
			t.Fatalf("normalizeGenericDocument success failed: %v", err)
		}
		if docID != "doc-storage" || normalizedDay != day || doc["partition_day"] != day {
			t.Fatalf("unexpected normalized document: id=%q day=%q doc=%#v", docID, normalizedDay, doc)
		}

		for _, value := range []string{
			day + "T12:00:00.123456Z",
			day + "T12:00:00Z",
			day + " 12:00:00",
			day,
		} {
			if _, err := parseTimeValue(value); err != nil {
				t.Fatalf("parseTimeValue(%q) failed: %v", value, err)
			}
		}
		if _, err := parseTimeValue("not-a-time"); err == nil {
			t.Fatalf("expected parseTimeValue to reject invalid time")
		}

		extractDoc := Document{"id": "extract-doc", "@timestamp": day + "T13:00:00Z"}
		extractedDay, err := extractEventDay(extractDoc)
		if err != nil || extractedDay != day {
			t.Fatalf("unexpected extracted event day: day=%q err=%v", extractedDay, err)
		}
		if _, err := extractEventDay(Document{"id": "extract-doc"}); err == nil {
			t.Fatalf("expected extractEventDay to reject documents without timestamps")
		}

		setTestRoute(s, "events", day, shardID, []string{"n1"})
		indexTestDocument(t, s, "events", day, shardID, "doc-storage", Document{
			"id":        "doc-storage",
			"timestamp": day + "T12:05:00Z",
			"message":   "storage coverage",
		})
		docs, err := s.fetchDocumentsByID("events", day, shardID, []string{"doc-storage"})
		if err != nil {
			t.Fatalf("fetchDocumentsByID: %v", err)
		}
		if len(docs) != 1 || docs["doc-storage"]["message"] != "storage coverage" {
			t.Fatalf("unexpected fetchDocumentsByID result: %#v", docs)
		}
		sizeBytes, err := s.shardStorageSize("events", day, shardID)
		if err != nil || sizeBytes == 0 {
			t.Fatalf("unexpected shardStorageSize result: size=%d err=%v", sizeBytes, err)
		}

		if err := s.removeLocalShardDay("events", day); err != nil {
			t.Fatalf("removeLocalShardDay: %v", err)
		}
		if _, err := os.Stat(s.shardDayPath("events", day)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected shard day path to be removed, got %v", err)
		}
	})

	t.Run("streamed shard helpers restore and replicate documents", func(t *testing.T) {
		receiver, _ := newNamedTestHTTPServer(t, "n1")
		route := RoutingEntry{
			IndexName: "events",
			Day:       "2026-03-24",
			ShardID:   1,
			Replicas:  []string{"n1"},
		}
		setTestRoute(receiver, route.IndexName, route.Day, route.ShardID, []string{"n1"})

		if _, err := normalizeStreamedBatchItem(route, Document{"message": "missing id"}); err == nil {
			t.Fatalf("expected normalizeStreamedBatchItem to reject documents without ids")
		}

		restored, err := receiver.restoreStreamedShardDocuments(route, func(onDoc func(Document) error) error {
			for i := 0; i <= shardSyncBatchSize; i++ {
				if err := onDoc(Document{
					"id":        fmt.Sprintf("stream-doc-%03d", i),
					"timestamp": route.Day + "T15:00:00Z",
					"message":   "streamed",
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("restoreStreamedShardDocuments success failed: %v", err)
		}
		if restored != shardSyncBatchSize+1 {
			t.Fatalf("expected %d restored docs, got %d", shardSyncBatchSize+1, restored)
		}

		if _, err := receiver.restoreStreamedShardDocuments(route, func(onDoc func(Document) error) error {
			return onDoc(Document{"timestamp": route.Day + "T15:00:00Z"})
		}); err == nil {
			t.Fatalf("expected restoreStreamedShardDocuments to fail on invalid streamed doc")
		}

		source, _ := newNamedTestHTTPServer(t, "n2")
		target, targetTS := newNamedTestHTTPServer(t, "n3")
		setTestRoute(source, route.IndexName, route.Day, route.ShardID, []string{"n2", "n3"})
		setTestRoute(target, route.IndexName, route.Day, route.ShardID, []string{"n3"})
		indexTestDocument(t, source, route.IndexName, route.Day, route.ShardID, "replica-doc", Document{
			"id":        "replica-doc",
			"timestamp": route.Day + "T16:00:00Z",
			"message":   "replicated",
		})

		source.membersMu.Lock()
		source.members["n3"] = NodeInfo{ID: "n3", Addr: targetTS.URL}
		source.membersMu.Unlock()

		sent, err := source.streamShardToReplica(RoutingEntry{
			IndexName: route.IndexName,
			Day:       route.Day,
			ShardID:   route.ShardID,
			Replicas:  []string{"n2", "n3"},
		}, "n3")
		if err != nil {
			t.Fatalf("streamShardToReplica success failed: %v", err)
		}
		if sent != 1 {
			t.Fatalf("expected 1 streamed doc, got %d", sent)
		}
		replicaDocs, err := target.dumpAllDocs(route.IndexName, route.Day, route.ShardID)
		if err != nil {
			t.Fatalf("dump replica docs after stream: %v", err)
		}
		if len(replicaDocs) != 1 || replicaDocs[0]["id"] != "replica-doc" {
			t.Fatalf("unexpected replica docs after stream: %#v", replicaDocs)
		}

		if _, err := source.streamShardToReplica(route, "missing-node"); err == nil {
			t.Fatalf("expected streamShardToReplica to fail for missing replica")
		}

		rejectingTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, internalIndexBatchResponse{OK: false})
		}))
		defer rejectingTS.Close()
		source.membersMu.Lock()
		source.members["n4"] = NodeInfo{ID: "n4", Addr: rejectingTS.URL}
		source.membersMu.Unlock()
		if _, err := source.streamShardToReplica(RoutingEntry{
			IndexName: route.IndexName,
			Day:       route.Day,
			ShardID:   route.ShardID,
			Replicas:  []string{"n2", "n4"},
		}, "n4"); err == nil {
			t.Fatalf("expected streamShardToReplica to fail when replica rejects repair batch")
		}
	})
}

func TestIngestAndRepairStateHelpers(t *testing.T) {
	t.Run("single document and bulk group cover local and remote primary branches", func(t *testing.T) {
		local, _ := newNamedTestHTTPServer(t, "n1")
		day := "2026-03-25"
		shardID := 2
		localRoute := RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n1"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		status, resp, err := local.indexSingleDocument(context.Background(), "events", day, shardID, localRoute, "local-doc", Document{
			"id":        "local-doc",
			"timestamp": day + "T08:00:00Z",
			"message":   "local primary",
		})
		if err != nil {
			t.Fatalf("local indexSingleDocument failed: %v", err)
		}
		if status != http.StatusOK || !resp.OK || resp.Primary != "n1" {
			t.Fatalf("unexpected local indexSingleDocument response: status=%d resp=%#v", status, resp)
		}

		remote, remoteTS := newNamedTestHTTPServer(t, "n2")
		setTestRoute(remote, "events", day, shardID, []string{"n2"})
		local.membersMu.Lock()
		local.members["n2"] = NodeInfo{ID: "n2", Addr: remoteTS.URL}
		local.membersMu.Unlock()

		status, resp, err = local.indexSingleDocument(context.Background(), "events", day, shardID, RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n2"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}, "remote-doc", Document{
			"id":        "remote-doc",
			"timestamp": day + "T08:05:00Z",
			"message":   "remote primary",
		})
		if err != nil {
			t.Fatalf("remote indexSingleDocument success failed: %v", err)
		}
		if status != http.StatusOK || !resp.OK {
			t.Fatalf("unexpected remote indexSingleDocument success response: status=%d resp=%#v", status, resp)
		}

		failingPrimaryTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusServiceUnavailable, internalIndexBatchResponse{
				OK:      false,
				Errors:  []string{"no quorum"},
				Indexed: 1,
			})
		}))
		defer failingPrimaryTS.Close()
		local.membersMu.Lock()
		local.members["n3"] = NodeInfo{ID: "n3", Addr: failingPrimaryTS.URL}
		local.membersMu.Unlock()
		status, resp, err = local.indexSingleDocument(context.Background(), "events", day, shardID, RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n3"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}, "remote-fail-doc", Document{
			"id":        "remote-fail-doc",
			"timestamp": day + "T08:10:00Z",
			"message":   "remote failure",
		})
		if err != nil {
			t.Fatalf("remote indexSingleDocument failure should return response, got err=%v", err)
		}
		if status != http.StatusServiceUnavailable || resp.OK || len(resp.Errors) != 1 {
			t.Fatalf("unexpected remote indexSingleDocument failure response: status=%d resp=%#v", status, resp)
		}

		remoteRejectTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, internalIndexBatchResponse{
				OK:     false,
				Errors: []string{"primary batch failed"},
			})
		}))
		defer remoteRejectTS.Close()
		local.membersMu.Lock()
		local.members["n4"] = NodeInfo{ID: "n4", Addr: remoteRejectTS.URL}
		local.membersMu.Unlock()
		rejectRoute := RoutingEntry{
			IndexName: "events",
			Day:       day,
			ShardID:   shardID,
			Replicas:  []string{"n4"},
		}
		err = local.ingestBulkShardGroupOnce(context.Background(), bulkShardGroup{
			indexName: "events",
			day:       day,
			shardID:   shardID,
			route:     rejectRoute,
			items: []bulkPreparedItem{{
				lineNo:    1,
				indexName: "events",
				day:       day,
				shardID:   shardID,
				item: internalIndexBatchItem{
					DocID: "bulk-reject-doc",
					Doc: Document{
						"id":        "bulk-reject-doc",
						"timestamp": day + "T08:15:00Z",
						"message":   "bulk reject",
					},
				},
			}},
		}, rejectRoute)
		if err == nil || !strings.Contains(err.Error(), "primary batch failed") {
			t.Fatalf("expected ingestBulkShardGroupOnce to surface primary batch failure, got %v", err)
		}

		if status, _, err := local.ingestDocument(context.Background(), "events", Document{
			"id":        "no-route-doc",
			"timestamp": day + "T08:20:00Z",
			"message":   "no route",
		}); err == nil || status != http.StatusServiceUnavailable {
			t.Fatalf("expected ingestDocument to fail without routing, got status=%d err=%v", status, err)
		}
	})

	t.Run("repair state helpers persist to etcd", func(t *testing.T) {
		cluster := startEmbeddedEtcd(t)
		client := newEmbeddedEtcdClient(t, cluster.endpoint)

		s := newEtcdBackedServer(t, cluster.endpoint, "n1", "both", "http://127.0.0.1:18111")
		mustRegisterAndLoadServerState(t, s)

		route := RoutingEntry{
			IndexName: "events",
			Day:       "2026-03-25",
			ShardID:   5,
			Replicas:  []string{"n1", "n2"},
			Version:   time.Now().UnixNano(),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		setTestRoute(s, route.IndexName, route.Day, route.ShardID, route.Replicas)

		if err := s.markReplicaRepairState(route, "n2"); err != nil {
			t.Fatalf("markReplicaRepairState: %v", err)
		}
		if !s.replicaNeedsRepair(route.IndexName, route.Day, route.ShardID, "n2") {
			t.Fatalf("expected replica repair state to be tracked locally")
		}
		resp, err := client.Get(context.Background(), s.replicaRepairKey(route.IndexName, route.Day, route.ShardID, "n2"))
		if err != nil {
			t.Fatalf("get replica repair key: %v", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("expected replica repair key in etcd, got %d entries", len(resp.Kvs))
		}

		if err := s.clearReplicaRepairState(route.IndexName, route.Day, route.ShardID, "n2"); err != nil {
			t.Fatalf("clearReplicaRepairState: %v", err)
		}
		if s.replicaNeedsRepair(route.IndexName, route.Day, route.ShardID, "n2") {
			t.Fatalf("expected replica repair state to be cleared locally")
		}
		resp, err = client.Get(context.Background(), s.replicaRepairKey(route.IndexName, route.Day, route.ShardID, "n2"))
		if err != nil {
			t.Fatalf("get cleared replica repair key: %v", err)
		}
		if len(resp.Kvs) != 0 {
			t.Fatalf("expected replica repair key to be deleted from etcd, got %d entries", len(resp.Kvs))
		}
	})
}
