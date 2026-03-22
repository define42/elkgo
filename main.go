package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"elkgo/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runContext(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	return runContext(context.Background(), args)
}

func runContext(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("elkgo", flag.ContinueOnError)

	var mode string
	var nodeID string
	var listen string
	var publicAddr string
	var dataDir string
	var etcdEndpointsRaw string
	var replicationFactor int
	var shardsPerDay int
	var shardSyncConcurrency int

	fs.StringVar(&mode, "mode", "both", "node|coordinator|both")
	fs.StringVar(&nodeID, "node-id", "n1", "node id")
	fs.StringVar(&listen, "listen", ":8081", "listen address")
	fs.StringVar(&publicAddr, "public-addr", envOr("ELKGO_PUBLIC_ADDR", ""), "public address advertised to the cluster")
	fs.StringVar(&dataDir, "data", "./data", "data directory")
	fs.StringVar(&etcdEndpointsRaw, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	fs.IntVar(&replicationFactor, "replication-factor", 3, "default replica count for bootstrap")
	fs.IntVar(&shardsPerDay, "shards-per-day", defaultEnvInt("ELKGO_SHARDS_PER_DAY", 48), "default shard count for newly bootstrapped index/day partitions")
	fs.IntVar(&shardSyncConcurrency, "shard-sync-concurrency", defaultEnvInt("ELKGO_SHARD_SYNC_CONCURRENCY", 0), "optional override for rebalance shard sync concurrency (0 = adaptive)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	endpoints := splitCSV(etcdEndpointsRaw)
	if len(endpoints) == 0 {
		return errors.New("at least one etcd endpoint is required")
	}

	s := server.New(server.Config{
		Mode:                 mode,
		NodeID:               nodeID,
		Listen:               listen,
		PublicAddr:           publicAddr,
		DataDir:              dataDir,
		ETCDEndpoints:        endpoints,
		ReplicationFactor:    replicationFactor,
		DefaultShardsPerDay:  shardsPerDay,
		ShardSyncConcurrency: shardSyncConcurrency,
	})
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-done:
		}
	}()
	defer close(done)
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

func defaultEnvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
