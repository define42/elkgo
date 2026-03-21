package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"elkgo/internal/server"
)

func main() {
	var mode string
	var nodeID string
	var listen string
	var publicAddr string
	var dataDir string
	var etcdEndpointsRaw string
	var replicationFactor int

	flag.StringVar(&mode, "mode", "both", "node|coordinator|both")
	flag.StringVar(&nodeID, "node-id", "n1", "node id")
	flag.StringVar(&listen, "listen", ":8081", "listen address")
	flag.StringVar(&publicAddr, "public-addr", envOr("ELKGO_PUBLIC_ADDR", ""), "public address advertised to the cluster")
	flag.StringVar(&dataDir, "data", "./data", "data directory")
	flag.StringVar(&etcdEndpointsRaw, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	flag.IntVar(&replicationFactor, "replication-factor", 3, "default replica count for bootstrap")
	flag.Parse()

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		log.Fatal("at least one etcd endpoint is required")
	}

	s := server.New(server.Config{
		Mode:              mode,
		NodeID:            nodeID,
		Listen:            listen,
		PublicAddr:        publicAddr,
		DataDir:           dataDir,
		ETCDEndpoints:     endpoints,
		ReplicationFactor: replicationFactor,
		AddTestData:       envBool("ELKGO_ADD_TEST_DATA"),
	})
	defer s.Close()

	if err := s.Run(); err != nil {
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

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	default:
		return false
	}
}
