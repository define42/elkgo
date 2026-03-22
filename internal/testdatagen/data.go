package testdatagen

import (
	"fmt"
	"time"
)

func buildTestDataDays(reference time.Time, dayCount int) []string {
	base := reference.UTC().Truncate(24 * time.Hour)
	days := make([]string, 0, dayCount)
	for offset := dayCount - 1; offset >= 0; offset-- {
		days = append(days, base.AddDate(0, 0, -offset).Format("2006-01-02"))
	}
	return days
}

func testDataDays(reference time.Time) []string {
	return buildTestDataDays(reference, DefaultDayCount)
}

func buildTestDataDocuments(day string, eventsPerDay int) []Document {
	services := []string{"api", "ingest", "membership", "storage", "frontend", "scheduler", "billing", "worker"}
	levels := []string{"info", "warn", "error"}
	environments := []string{"prod", "stage", "dev"}
	regions := []string{"eu-west", "us-east", "ap-south"}
	issues := []struct {
		title   string
		message string
		tag     string
	}{
		{title: "API timeout", message: "timeout talking to etcd during bootstrap", tag: "timeouts"},
		{title: "Indexer recovered", message: "replica repair completed for shard sync", tag: "repair"},
		{title: "Search latency spike", message: "query latency exceeded service threshold", tag: "latency"},
		{title: "Node joined cluster", message: "new replica node registered with etcd lease", tag: "cluster"},
		{title: "Disk pressure", message: "bleve segment compaction delayed due to disk pressure", tag: "storage"},
		{title: "Customer search error", message: "customer search request returned partial shard failures", tag: "errors"},
		{title: "Shard rebalanced", message: "primary ownership moved after membership change", tag: "routing"},
		{title: "Worker backlog", message: "background processing queue depth crossed warning threshold", tag: "backlog"},
	}

	base, err := time.Parse("2006-01-02", day)
	if err != nil {
		base = time.Now().UTC().Truncate(24 * time.Hour)
	}
	base = base.UTC()

	docs := make([]Document, 0, eventsPerDay)
	for i := 0; i < eventsPerDay; i++ {
		issue := issues[i%len(issues)]
		service := services[i%len(services)]
		level := levels[(i/3)%len(levels)]
		environment := environments[(i/7)%len(environments)]
		region := regions[(i/11)%len(regions)]
		timestamp := base.Add(time.Duration((i*7)%86400) * time.Second).Format(time.RFC3339)

		docs = append(docs, Document{
			"id":        fmt.Sprintf("evt-%05d", i+1),
			"timestamp": timestamp,
			"title":     issue.title,
			"service":   service,
			"level":     level,
			"message":   fmt.Sprintf("%s on %s in %s", issue.message, service, region),
			"tags": []string{
				environment,
				service,
				issue.tag,
				level,
			},
			"count": 1 + (i % 9),
			"score": 55 + (i % 45),
		})
	}
	return docs
}

func testDataDocuments(day string) []Document {
	return buildTestDataDocuments(day, DefaultEventsPerDay)
}
