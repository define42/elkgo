package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

	"elkgo/internal/testdatagen"
)

func main() {
	var serverURL string
	var indexName string
	var etcdEndpointsRaw string
	var replicationFactor int
	var waitForMembers int
	var markerVersion string

	flag.StringVar(&serverURL, "server-url", envOr("ELKGO_SERVER_URL", "http://127.0.0.1:8081"), "coordinator URL used for bootstrap and bulk ingest")
	flag.StringVar(&indexName, "index", envOr("ELKGO_TESTDATA_INDEX", testdatagen.DefaultIndexName), "index to seed")
	flag.StringVar(&etcdEndpointsRaw, "etcd-endpoints", envOr("ELKGO_ETCD_ENDPOINTS", "http://127.0.0.1:2379"), "comma-separated etcd endpoints")
	flag.IntVar(&replicationFactor, "replication-factor", 3, "replication factor used for bootstrap")
	flag.IntVar(&waitForMembers, "wait-for-members", 0, "minimum cluster members to wait for before seeding; defaults to replication factor")
	flag.StringVar(&markerVersion, "marker-version", envOr("ELKGO_TESTDATA_MARKER_VERSION", testdatagen.DefaultMarkerVersion), "seed marker version in etcd")
	flag.Parse()

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		log.Fatal("at least one etcd endpoint is required")
	}

	generator := testdatagen.New(testdatagen.Config{
		ServerURL:         serverURL,
		ETCDEndpoints:     endpoints,
		IndexName:         indexName,
		ReplicationFactor: replicationFactor,
		WaitForMembers:    waitForMembers,
		MarkerVersion:     markerVersion,
	})

	if err := generator.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
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
