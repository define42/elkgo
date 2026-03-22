package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"elkgo/internal/testdatagen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("elkgo-testdata", flag.ContinueOnError)

	var serverURL string
	var indexName string
	var etcdEndpointsRaw string
	var dayCount int
	var eventsPerDay int
	var replicationFactor int
	var waitForMembers int
	var markerVersion string

	fs.StringVar(&serverURL, "server-url", envOr("ELKGO_SERVER_URL", "http://127.0.0.1:8081"), "coordinator URL used for bootstrap and bulk ingest")
	fs.StringVar(&indexName, "index", envOr("ELKGO_TESTDATA_INDEX", testdatagen.DefaultIndexName), "index to seed")
	fs.StringVar(&etcdEndpointsRaw, "etcd-endpoints", envOr("ELKGO_ETCD_ENDPOINTS", "http://127.0.0.1:2379"), "comma-separated etcd endpoints")
	fs.IntVar(&dayCount, "days-back", envOrInt("ELKGO_TESTDATA_DAYS_BACK", testdatagen.DefaultDayCount), "number of days to seed including today")
	fs.IntVar(&eventsPerDay, "events-per-day", envOrInt("ELKGO_TESTDATA_EVENTS_PER_DAY", testdatagen.DefaultEventsPerDay), "number of events to generate for each day")
	fs.IntVar(&replicationFactor, "replication-factor", 3, "replication factor used for bootstrap")
	fs.IntVar(&waitForMembers, "wait-for-members", 0, "minimum cluster members to wait for before seeding; defaults to replication factor")
	fs.StringVar(&markerVersion, "marker-version", envOr("ELKGO_TESTDATA_MARKER_VERSION", testdatagen.DefaultMarkerVersion), "seed marker version in etcd")
	if err := fs.Parse(args); err != nil {
		return err
	}

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		return errors.New("at least one etcd endpoint is required")
	}

	generator := testdatagen.New(testdatagen.Config{
		ServerURL:         serverURL,
		ETCDEndpoints:     endpoints,
		IndexName:         indexName,
		DayCount:          dayCount,
		EventsPerDay:      eventsPerDay,
		ReplicationFactor: replicationFactor,
		WaitForMembers:    waitForMembers,
		MarkerVersion:     markerVersion,
	})

	return generator.Run(context.Background())
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
