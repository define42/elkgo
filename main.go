package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strings"

	"elkgo/internal/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("elkgo", flag.ContinueOnError)

	var mode string
	var nodeID string
	var listen string
	var publicAddr string
	var dataDir string
	var etcdEndpointsRaw string
	var replicationFactor int

	fs.StringVar(&mode, "mode", "both", "node|coordinator|both")
	fs.StringVar(&nodeID, "node-id", "n1", "node id")
	fs.StringVar(&listen, "listen", ":8081", "listen address")
	fs.StringVar(&publicAddr, "public-addr", envOr("ELKGO_PUBLIC_ADDR", ""), "public address advertised to the cluster")
	fs.StringVar(&dataDir, "data", "./data", "data directory")
	fs.StringVar(&etcdEndpointsRaw, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	fs.IntVar(&replicationFactor, "replication-factor", 3, "default replica count for bootstrap")
	if err := fs.Parse(args); err != nil {
		return err
	}

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		return errors.New("at least one etcd endpoint is required")
	}

	s := server.New(server.Config{
		Mode:              mode,
		NodeID:            nodeID,
		Listen:            listen,
		PublicAddr:        publicAddr,
		DataDir:           dataDir,
		ETCDEndpoints:     endpoints,
		ReplicationFactor: replicationFactor,
	})
	defer s.Close()

	return s.Run()
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
