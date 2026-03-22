package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestHandleSearchShardAndFetchDocs_UseWALSource(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 0
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	indexTestDocument(t, s, "events", day, shardID, "wal-search", Document{
		"id":        "wal-search",
		"timestamp": day + "T13:00:00Z",
		"message":   "wal backed search hit",
		"service":   "api",
	})

	searchReqBody := `{"index_name":"events","day":"2026-03-21","shard_id":0,"query":"wal backed","k":5,"fetch_docs":true}`
	resp, err := http.Post(ts.URL+"/internal/search_shard", "application/json", strings.NewReader(searchReqBody))
	if err != nil {
		t.Fatalf("search shard request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected search shard status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	var searchResp SearchShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		t.Fatalf("decode search shard response: %v", err)
	}
	if len(searchResp.Hits) != 1 {
		t.Fatalf("expected 1 search shard hit, got %d", len(searchResp.Hits))
	}
	if searchResp.Hits[0].Source["message"] != "wal backed search hit" {
		t.Fatalf("unexpected WAL-backed source document: %#v", searchResp.Hits[0].Source)
	}

	fetchBody, err := json.Marshal(FetchDocsRequest{
		IndexName: "events",
		Day:       day,
		ShardID:   shardID,
		DocIDs:    []string{"wal-search"},
	})
	if err != nil {
		t.Fatalf("marshal fetch docs request: %v", err)
	}
	resp, err = http.Post(ts.URL+"/internal/fetch_docs", "application/json", bytes.NewReader(fetchBody))
	if err != nil {
		t.Fatalf("fetch docs request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected fetch docs status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	var fetchResp FetchDocsResponse
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		t.Fatalf("decode fetch docs response: %v", err)
	}
	if len(fetchResp.Docs) != 1 || fetchResp.Docs[0].Source["service"] != "api" {
		t.Fatalf("unexpected fetch docs response: %#v", fetchResp)
	}

	forbiddenBody, err := json.Marshal(FetchDocsRequest{
		IndexName: "events",
		Day:       day,
		ShardID:   1,
		DocIDs:    []string{"wal-search"},
	})
	if err != nil {
		t.Fatalf("marshal forbidden fetch docs request: %v", err)
	}
	resp, err = http.Post(ts.URL+"/internal/fetch_docs", "application/json", bytes.NewReader(forbiddenBody))
	if err != nil {
		t.Fatalf("forbidden fetch docs request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected fetch docs status 403 for non-replica shard, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
}

func TestHandleIndexAndInternalIndex_SuccessPaths(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 3
	docID := findDocIDForShard(t, shardID, "http-index")
	setTestRoute(s, "events", day, shardID, []string{"n1"})

	publicDoc := Document{
		"id":        docID,
		"timestamp": day + "T12:00:00Z",
		"message":   "indexed through public API",
	}
	body, err := json.Marshal(publicDoc)
	if err != nil {
		t.Fatalf("marshal public index doc: %v", err)
	}
	resp, err := http.Post(ts.URL+"/index?index=events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("public index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected public index status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	internalDoc := internalIndexRequest{
		IndexName: "events",
		Day:       day,
		ShardID:   shardID,
		DocID:     "internal-doc",
		Doc: Document{
			"id":        "internal-doc",
			"timestamp": day + "T12:05:00Z",
			"message":   "indexed through internal API",
		},
	}
	internalBody, err := json.Marshal(internalDoc)
	if err != nil {
		t.Fatalf("marshal internal index doc: %v", err)
	}
	resp, err = http.Post(ts.URL+"/internal/index", "application/json", bytes.NewReader(internalBody))
	if err != nil {
		t.Fatalf("internal index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected internal index status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	docs, err := s.dumpAllDocs("events", day, shardID)
	if err != nil {
		t.Fatalf("dump docs after index handlers: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs after index handlers, got %#v", docs)
	}
}

func TestHandleIndexAndInternalIndex_ValidationAndRemotePrimary(t *testing.T) {
	remote, remoteTS := newNamedTestHTTPServer(t, "n2")
	coordinator, coordinatorTS := newNamedTestHTTPServer(t, "n1")

	day := "2026-03-21"
	shardID := 7
	docID := findDocIDForShard(t, shardID, "remote-primary-http")
	setTestRoute(coordinator, "events", day, shardID, []string{"n2"})
	setTestRoute(remote, "events", day, shardID, []string{"n2"})

	coordinator.membersMu.Lock()
	coordinator.members["n2"] = NodeInfo{ID: "n2", Addr: remoteTS.URL}
	coordinator.membersMu.Unlock()

	body := []byte(`{"id":"` + docID + `","timestamp":"2026-03-21T12:30:00Z","message":"remote primary path"}`)
	resp, err := http.Post(coordinatorTS.URL+"/index?index=events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("remote primary public index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected remote primary index status 200, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	docs, err := remote.dumpAllDocs("events", day, shardID)
	if err != nil {
		t.Fatalf("dump remote primary docs: %v", err)
	}
	if len(docs) != 1 || docs[0]["message"] != "remote primary path" {
		t.Fatalf("unexpected remote primary docs: %#v", docs)
	}

	resp, err = http.Post(coordinatorTS.URL+"/index", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("missing-index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected index status 400 for missing index, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	req, err := http.NewRequest(http.MethodGet, coordinatorTS.URL+"/index?index=events", nil)
	if err != nil {
		t.Fatalf("new GET /index request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected index status 405 for GET, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	req, err = http.NewRequest(http.MethodGet, coordinatorTS.URL+"/internal/index", nil)
	if err != nil {
		t.Fatalf("new GET /internal/index request: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /internal/index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected internal index status 405 for GET, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
}

func TestSnapshotHandlers_RoundTripIncludesWALSidecar(t *testing.T) {
	source, sourceTS := newNamedTestHTTPServer(t, "n1")
	target, targetTS := newNamedTestHTTPServer(t, "n2")

	day := "2026-03-21"
	shardID := 0
	setTestRoute(source, "events", day, shardID, []string{"n1"})
	setTestRoute(target, "events", day, shardID, []string{"n2"})

	indexTestDocument(t, source, "events", day, shardID, "snap-doc", Document{
		"id":        "snap-doc",
		"timestamp": day + "T14:00:00Z",
		"message":   "snapshot carries wal",
		"service":   "snapshotter",
	})

	resp, err := http.Get(sourceTS.URL + "/internal/snapshot_shard?index=events&day=" + url.QueryEscape(day) + "&shard=0")
	if err != nil {
		t.Fatalf("snapshot shard request failed: %v", err)
	}
	snapshotBytes, err := ioReadAllAndClose(resp)
	if err != nil {
		t.Fatalf("read snapshot archive: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected snapshot shard status 200, got %d body=%q", resp.StatusCode, string(snapshotBytes))
	}

	req, err := http.NewRequest(http.MethodPost, targetTS.URL+"/internal/install_snapshot_shard?index=events&day="+url.QueryEscape(day)+"&shard=0", bytes.NewReader(snapshotBytes))
	if err != nil {
		t.Fatalf("new install snapshot request: %v", err)
	}
	req.Header.Set("Content-Type", "application/zip")
	installResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("install snapshot request failed: %v", err)
	}
	defer installResp.Body.Close()
	if installResp.StatusCode != http.StatusOK {
		t.Fatalf("expected install snapshot status 200, got %d body=%q", installResp.StatusCode, readAllAndClose(t, installResp))
	}

	docs, err := target.dumpAllDocs("events", day, shardID)
	if err != nil {
		t.Fatalf("dump restored target shard: %v", err)
	}
	if len(docs) != 1 || docs[0]["message"] != "snapshot carries wal" {
		t.Fatalf("unexpected restored docs after snapshot install: %#v", docs)
	}

	sourcePath := target.shardSourceSegmentPath("events", day, shardID, currentSourceSegment)
	if info, err := os.Stat(sourcePath); err != nil || info.Size() == 0 {
		t.Fatalf("expected restored WAL sidecar at %s, stat err=%v size=%d", sourcePath, err, func() int64 {
			if info == nil {
				return 0
			}
			return info.Size()
		}())
	}
}

func TestSnapshotHandlers_ValidationPaths(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	day := "2026-03-21"
	shardID := 0

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/internal/snapshot_shard?index=events&day="+url.QueryEscape(day)+"&shard=0", nil)
	if err != nil {
		t.Fatalf("new snapshot method request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("snapshot wrong-method request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected snapshot status 405, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	resp, err = http.Get(ts.URL + "/internal/snapshot_shard?index=events&day=" + url.QueryEscape(day) + "&shard=0")
	if err != nil {
		t.Fatalf("snapshot forbidden request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected snapshot status 403 for unassigned shard, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	resp, err = http.Post(ts.URL+"/internal/install_snapshot_shard?index=events&day="+url.QueryEscape(day)+"&shard=bad", "application/zip", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("install snapshot invalid-shard request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected install snapshot status 400 for invalid shard, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	resp, err = http.Post(ts.URL+"/internal/install_snapshot_shard?index=events&day="+url.QueryEscape(day)+"&shard=0", "application/zip", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("install snapshot forbidden request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected install snapshot status 403 for unassigned shard, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	setTestRoute(s, "events", day, shardID, []string{"n1"})
	resp, err = http.Post(ts.URL+"/internal/install_snapshot_shard?index=events&day="+url.QueryEscape(day)+"&shard=0", "application/zip", bytes.NewReader([]byte("not-a-zip")))
	if err != nil {
		t.Fatalf("install snapshot bad-body request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected install snapshot status 500 for invalid archive, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}
}

func TestHandleFetchDocsAndInternalIndex_BadRequestPaths(t *testing.T) {
	s, ts := newTestHTTPServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/internal/fetch_docs", nil)
	if err != nil {
		t.Fatalf("new GET /internal/fetch_docs request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /internal/fetch_docs request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected fetch docs status 405 for GET, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	resp, err = http.Post(ts.URL+"/internal/fetch_docs", "application/json", strings.NewReader("{bad json"))
	if err != nil {
		t.Fatalf("bad JSON /internal/fetch_docs request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected fetch docs status 400 for bad JSON, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	resp, err = http.Post(ts.URL+"/internal/index", "application/json", strings.NewReader(`{"index_name":"events","day":"2026-03-21","shard_id":0,"doc":{}}`))
	if err != nil {
		t.Fatalf("unassigned /internal/index request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected internal index status 403 for unassigned replica, got %d body=%q", resp.StatusCode, readAllAndClose(t, resp))
	}

	_ = s
}

func TestReadSourceDocumentFromFields_RequiresSourcePointer(t *testing.T) {
	_, err := readSourceDocumentFromFields(nil, "legacy-doc", map[string]interface{}{
		"message": "legacy stored source",
	})
	if err == nil || !strings.Contains(err.Error(), "missing source pointer for legacy-doc") {
		t.Fatalf("expected missing source pointer error, got %v", err)
	}
}

func TestInt64FromValueAndSourcePointerParsing(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want int64
		ok   bool
	}{
		{name: "int", in: int(7), want: 7, ok: true},
		{name: "int64", in: int64(8), want: 8, ok: true},
		{name: "uint32", in: uint32(9), want: 9, ok: true},
		{name: "float64", in: float64(10), want: 10, ok: true},
		{name: "json-number", in: json.Number("11"), want: 11, ok: true},
		{name: "string", in: "12", want: 12, ok: true},
		{name: "nil", in: nil, ok: false},
		{name: "unsupported", in: []string{"x"}, ok: false},
	}

	for _, tc := range cases {
		got, ok := int64FromValue(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got (%d,%v) want (%d,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}

	pointer, ok, err := sourcePointerFromFields(map[string]interface{}{
		sourceFieldSegment:          float64(1),
		sourceFieldOffset:           float64(128),
		sourceFieldCompressedLength: float64(64),
		sourceFieldRawLength:        float64(256),
	})
	if err != nil || !ok {
		t.Fatalf("expected valid pointer parse, got pointer=%#v ok=%v err=%v", pointer, ok, err)
	}
	if pointer.Segment != 1 || pointer.Offset != 128 || pointer.CompressedLength != 64 || pointer.RawLength != 256 {
		t.Fatalf("unexpected parsed pointer: %#v", pointer)
	}

	if _, ok, err := sourcePointerFromFields(map[string]interface{}{
		sourceFieldSegment: float64(1),
		sourceFieldOffset:  float64(128),
	}); err == nil || ok {
		t.Fatalf("expected incomplete source pointer parse to fail, got ok=%v err=%v", ok, err)
	}
}

func TestSourceRecordRoundTripAndCorruption(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "source-record-*.wal")
	if err != nil {
		t.Fatalf("create temp WAL: %v", err)
	}
	path := file.Name()

	raw := []byte(`{"id":"rec-1","message":"round trip"}`)
	compressed := compressSourceRecord(raw)
	if err := writeSourceRecord(file, raw, compressed); err != nil {
		t.Fatalf("write source record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp WAL: %v", err)
	}

	reader, err := os.Open(path)
	if err != nil {
		t.Fatalf("open temp WAL: %v", err)
	}

	pointer := sourcePointer{
		Segment:          1,
		Offset:           0,
		CompressedLength: uint64(len(compressed)),
		RawLength:        uint64(len(raw)),
	}
	got, err := readSourceRecord(reader, pointer)
	if err != nil {
		t.Fatalf("read source record: %v", err)
	}
	if string(got) != string(raw) {
		t.Fatalf("unexpected source record payload: got %q want %q", got, raw)
	}
	_ = reader.Close()

	corrupted, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}
	if _, err := corrupted.WriteAt([]byte{0xFF}, sourceRecordHeaderSize); err != nil {
		t.Fatalf("corrupt WAL payload: %v", err)
	}
	if err := corrupted.Close(); err != nil {
		t.Fatalf("close corrupted WAL: %v", err)
	}

	reader, err = os.Open(path)
	if err != nil {
		t.Fatalf("reopen corrupted WAL: %v", err)
	}
	defer reader.Close()
	if _, err := readSourceRecord(reader, pointer); err == nil {
		t.Fatalf("expected checksum mismatch after WAL corruption")
	}
}

func TestReadSourceRecord_HeaderValidation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "source-header-*.wal")
	if err != nil {
		t.Fatalf("create temp WAL: %v", err)
	}
	path := file.Name()

	raw := []byte(`{"id":"rec-2","message":"header validation"}`)
	compressed := compressSourceRecord(raw)
	if err := writeSourceRecord(file, raw, compressed); err != nil {
		t.Fatalf("write source record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp WAL: %v", err)
	}

	corruptAt := func(offset int64, value byte) {
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("open WAL for header corruption: %v", err)
		}
		if _, err := f.WriteAt([]byte{value}, offset); err != nil {
			_ = f.Close()
			t.Fatalf("corrupt WAL header: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close corrupted WAL header: %v", err)
		}
	}

	pointer := sourcePointer{
		Segment:          1,
		Offset:           0,
		CompressedLength: uint64(len(compressed)),
		RawLength:        uint64(len(raw)),
	}

	corruptAt(0, 'X')
	reader, err := os.Open(path)
	if err != nil {
		t.Fatalf("open invalid-magic WAL: %v", err)
	}
	if _, err := readSourceRecord(reader, pointer); err == nil {
		t.Fatalf("expected invalid magic error")
	}
	_ = reader.Close()

	file, err = os.Create(path)
	if err != nil {
		t.Fatalf("recreate WAL for version test: %v", err)
	}
	if err := writeSourceRecord(file, raw, compressed); err != nil {
		t.Fatalf("rewrite source record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close rewritten WAL: %v", err)
	}
	corruptAt(4, 99)
	reader, err = os.Open(path)
	if err != nil {
		t.Fatalf("open invalid-version WAL: %v", err)
	}
	defer reader.Close()
	if _, err := readSourceRecord(reader, pointer); err == nil {
		t.Fatalf("expected unsupported version error")
	}
}

func TestRemoveLocalShardFilesAndShardStorageSize(t *testing.T) {
	s := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer s.Close()

	day := "2026-03-21"
	shardID := 0
	if err := s.indexBatchLocal("events", day, shardID, []internalIndexBatchItem{{
		DocID: "cleanup-doc",
		Doc: Document{
			"id":        "cleanup-doc",
			"timestamp": day + "T15:00:00Z",
			"message":   "cleanup path",
		},
	}}); err != nil {
		t.Fatalf("index batch local: %v", err)
	}

	sizeBefore, err := s.shardStorageSize("events", day, shardID)
	if err != nil {
		t.Fatalf("shard storage size before cleanup: %v", err)
	}
	if sizeBefore == 0 {
		t.Fatalf("expected shard storage size to include Bleve+WAL data")
	}

	if err := s.removeLocalShardFiles("events", day, shardID); err != nil {
		t.Fatalf("remove local shard files: %v", err)
	}

	if _, err := os.Stat(s.shardIndexPath("events", day, shardID)); !os.IsNotExist(err) {
		t.Fatalf("expected shard index to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(s.shardSourceSegmentPath("events", day, shardID, currentSourceSegment)); !os.IsNotExist(err) {
		t.Fatalf("expected shard WAL to be removed, stat err=%v", err)
	}

	if _, err := s.shardStorageSize("events", day, shardID); !errors.Is(err, errShardIndexMissing) {
		t.Fatalf("expected shard storage size to report missing after cleanup, got %v", err)
	}
}

func TestInstallShardSnapshot_ReplaceExistingData(t *testing.T) {
	source := New(Config{
		Mode:              "both",
		NodeID:            "n1",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer source.Close()

	target := New(Config{
		Mode:              "both",
		NodeID:            "n2",
		Listen:            ":0",
		DataDir:           t.TempDir(),
		ReplicationFactor: 1,
	})
	defer target.Close()

	day := "2026-03-21"
	shardID := 0
	indexName := "events"

	if err := source.indexBatchLocal(indexName, day, shardID, []internalIndexBatchItem{{
		DocID: "source-doc",
		Doc: Document{
			"id":        "source-doc",
			"timestamp": day + "T16:00:00Z",
			"message":   "source snapshot data",
		},
	}}); err != nil {
		t.Fatalf("index source doc: %v", err)
	}
	if err := target.indexBatchLocal(indexName, day, shardID, []internalIndexBatchItem{{
		DocID: "target-doc",
		Doc: Document{
			"id":        "target-doc",
			"timestamp": day + "T16:05:00Z",
			"message":   "stale target data",
		},
	}}); err != nil {
		t.Fatalf("index target doc: %v", err)
	}

	idx, err := source.openExistingShardIndex(indexName, day, shardID)
	if err != nil {
		t.Fatalf("open source shard index: %v", err)
	}

	archivePath := filepath.Join(t.TempDir(), "snapshot.zip")
	if err := source.writeShardSnapshotArchive(indexName, day, shardID, idx, archivePath); err != nil {
		t.Fatalf("write snapshot archive: %v", err)
	}

	archiveFile, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open snapshot archive: %v", err)
	}
	defer archiveFile.Close()

	if err := target.installShardSnapshot(indexName, day, shardID, archiveFile, true); err != nil {
		t.Fatalf("install snapshot archive: %v", err)
	}

	docs, err := target.dumpAllDocs(indexName, day, shardID)
	if err != nil {
		t.Fatalf("dump target docs after snapshot replace: %v", err)
	}
	if len(docs) != 1 || docs[0]["id"] != "source-doc" || docs[0]["message"] != "source snapshot data" {
		t.Fatalf("unexpected docs after snapshot replace: %#v", docs)
	}
}

func TestShardCountsFromRoutingAndCompareSearchRefs(t *testing.T) {
	routes := map[string]RoutingEntry{
		routingMapKey("events", "2026-03-21", 0): {IndexName: "events", Day: "2026-03-21", ShardID: 0},
		routingMapKey("events", "2026-03-21", 7): {IndexName: "events", Day: "2026-03-21", ShardID: 7},
		routingMapKey("logs", "2026-03-22", 2):   {IndexName: "logs", Day: "2026-03-22", ShardID: 2},
	}
	counts := shardCountsFromRouting(routes)
	if !reflect.DeepEqual(counts, map[string]int{
		partitionDayKey("events", "2026-03-21"): 8,
		partitionDayKey("logs", "2026-03-22"):   3,
	}) {
		t.Fatalf("unexpected shard counts: %#v", counts)
	}

	betterScore := searchHitRef{DocID: "a", Score: 2.0}
	worseScore := searchHitRef{DocID: "b", Score: 1.0}
	if got := compareSearchRefs(betterScore, worseScore); got <= 0 {
		t.Fatalf("expected higher score to compare greater, got %d", got)
	}

	tieLowerDocID := searchHitRef{DocID: "a", Score: 1.0}
	tieHigherDocID := searchHitRef{DocID: "b", Score: 1.0}
	if got := compareSearchRefs(tieLowerDocID, tieHigherDocID); got <= 0 {
		t.Fatalf("expected lexicographically lower doc id to compare greater on score tie, got %d", got)
	}
}

func ioReadAllAndClose(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
