package testdatagen

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPostDocumentsInBatches_SplitsLargeBulkRequests(t *testing.T) {
	g := New(Config{})

	docs := make([]Document, 0, 2505)
	for i := 0; i < 2505; i++ {
		docs = append(docs, Document{
			"id":        "evt-" + strconv.Itoa(i+1),
			"timestamp": "2026-03-21T12:00:00Z",
		})
	}

	requests := 0
	lineCounts := make([]int, 0, 3)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		requests++
		lines := bytes.Count(body, []byte{'\n'})
		lineCounts = append(lineCounts, lines)
		writeJSON(t, w, http.StatusOK, map[string]any{
			"ok":      true,
			"indexed": lines,
			"failed":  0,
			"errors":  []string{},
		})
	}))
	defer ts.Close()

	indexed, err := g.postDocumentsInBatches(context.Background(), ts.URL+"/bulk?index=events", "2026-03-21", docs, 1000)
	if err != nil {
		t.Fatalf("postDocumentsInBatches returned error: %v", err)
	}
	if indexed != len(docs) {
		t.Fatalf("expected indexed=%d, got %d", len(docs), indexed)
	}
	if requests != 3 {
		t.Fatalf("expected 3 batched requests, got %d", requests)
	}
	if !reflect.DeepEqual(lineCounts, []int{1000, 1000, 505}) {
		t.Fatalf("unexpected batch sizes: %#v", lineCounts)
	}
}

func TestPostDocumentsInBatches_LogsProgressPerThousand(t *testing.T) {
	g := New(Config{})

	docs := make([]Document, 0, 2005)
	for i := 0; i < 2005; i++ {
		docs = append(docs, Document{
			"id":        "evt-" + strconv.Itoa(i+1),
			"timestamp": "2026-03-21T12:00:00Z",
		})
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		lines := bytes.Count(body, []byte{'\n'})
		writeJSON(t, w, http.StatusOK, map[string]any{
			"ok":      true,
			"indexed": lines,
			"failed":  0,
			"errors":  []string{},
		})
	}))
	defer ts.Close()

	var logs bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	_, err := g.postDocumentsInBatches(context.Background(), ts.URL+"/bulk?index=events", "2026-03-21", docs, 1000)
	if err != nil {
		t.Fatalf("postDocumentsInBatches returned error: %v", err)
	}

	output := logs.String()
	for _, want := range []string{
		"indexed=1000/2005",
		"indexed=2000/2005",
		"indexed=2005/2005",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected logs to contain %q, got %q", want, output)
		}
	}
}

func TestTestDataDocuments_GeneratesTenThousandEvents(t *testing.T) {
	day := "2026-03-21"
	docs := testDataDocuments(day)

	if len(docs) != DefaultEventsPerDay {
		t.Fatalf("expected %d docs, got %d", DefaultEventsPerDay, len(docs))
	}

	seen := make(map[string]struct{}, len(docs))
	for i, doc := range docs {
		id, ok := doc["id"].(string)
		if !ok || id == "" {
			t.Fatalf("doc %d missing string id: %#v", i, doc["id"])
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id found: %s", id)
		}
		seen[id] = struct{}{}

		ts, ok := doc["timestamp"].(string)
		if !ok || len(ts) < len(day) || ts[:len(day)] != day {
			t.Fatalf("doc %d timestamp does not match requested day: %#v", i, doc["timestamp"])
		}
	}

	if _, ok := seen["evt-00001"]; !ok {
		t.Fatalf("expected first deterministic id to exist")
	}
	if _, ok := seen["evt-10000"]; !ok {
		t.Fatalf("expected last deterministic id to exist")
	}
}

func TestTestDataDays_ReturnsLastSevenDays(t *testing.T) {
	reference := time.Date(2026, 3, 21, 14, 30, 0, 0, time.UTC)

	days := testDataDays(reference)
	want := []string{
		"2026-03-15",
		"2026-03-16",
		"2026-03-17",
		"2026-03-18",
		"2026-03-19",
		"2026-03-20",
		"2026-03-21",
	}

	if !reflect.DeepEqual(days, want) {
		t.Fatalf("unexpected test data days: got %#v want %#v", days, want)
	}
}

func TestTestDataGenerator_CoversSeventyThousandEventsAcrossSevenDays(t *testing.T) {
	days := testDataDays(time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC))
	total := 0

	for _, day := range days {
		docs := testDataDocuments(day)
		total += len(docs)

		if len(docs) != DefaultEventsPerDay {
			t.Fatalf("expected %d docs for %s, got %d", DefaultEventsPerDay, day, len(docs))
		}
		if got := docs[0]["timestamp"]; got == nil || got.(string)[:len(day)] != day {
			t.Fatalf("expected first timestamp for %s to stay on that day, got %#v", day, got)
		}
	}

	if total != DefaultTotalEvents {
		t.Fatalf("expected %d total docs, got %d", DefaultTotalEvents, total)
	}
}

func TestBuildTestDataDays_UsesConfiguredCount(t *testing.T) {
	days := buildTestDataDays(time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC), 3)
	want := []string{
		"2026-03-19",
		"2026-03-20",
		"2026-03-21",
	}

	if !reflect.DeepEqual(days, want) {
		t.Fatalf("unexpected configured test data days: got %#v want %#v", days, want)
	}
}

func TestBuildTestDataDocuments_UsesConfiguredEventCount(t *testing.T) {
	docs := buildTestDataDocuments("2026-03-21", 12)

	if len(docs) != 12 {
		t.Fatalf("expected 12 docs, got %d", len(docs))
	}
	if got := docs[0]["id"]; got != "evt-00001" {
		t.Fatalf("expected first deterministic id, got %#v", got)
	}
	if got := docs[len(docs)-1]["id"]; got != "evt-00012" {
		t.Fatalf("expected last deterministic id, got %#v", got)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
