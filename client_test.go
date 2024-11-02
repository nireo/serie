package serie

import (
	"fmt"
	"testing"
	"time"
)

func TestClientIntegration(t *testing.T) {
	nodes := []struct {
		addr     string
		db       *TSMTree
		service  *HttpService
		isClosed bool
	}{
		{addr: "127.0.0.1:8081"},
		{addr: "127.0.0.1:8082"},
		{addr: "127.0.0.1:8083"},
		{addr: "127.0.0.1:8084"},
		{addr: "127.0.0.1:8085"},
		{addr: "127.0.0.1:8086"},
	}

	for i := range nodes {
		db, err := NewTSMTree(Config{
			MaxMemSize:    1024 * 1024, // 1MB for testing
			DataDir:       fmt.Sprintf("./testdata/node%d", i),
			FlushInterval: time.Second * 5,
		})
		if err != nil {
			t.Fatalf("failed to create TSMTree for node %d: %v", i, err)
		}
		nodes[i].db = db

		service := NewHttpService(nodes[i].addr, db)
		if err := service.Start(); err != nil {
			t.Fatalf("failed to start HTTP service for node %d: %v", i, err)
		}
		nodes[i].service = service
	}

	distributor := NewDistributor(10, 2)
	distributor.ClusterFilePath = "./testdata/cluster.json"
	distributor.HeartbeatTimeout = time.Second * 5

	for _, node := range nodes {
		err := distributor.AddNode(&Member{
			Name: fmt.Sprintf("node-%s", node.addr),
			Addr: node.addr,
		})
		if err != nil {
			t.Fatalf("failed to add node to distributor: %v", err)
		}
	}

	heartbeat := distributor.InitializeHeartbeat(time.Second * 5)
	defer heartbeat.Stop()

	client := &Client{distributor: distributor}

	now := time.Now().Unix()
	hourAgo := now - 3600

	// Test cases
	tests := []struct {
		name        string
		writePoints []Point
		queryStr    string
		verify      func(t *testing.T, results []QueryResult)
	}{
		{
			name: "basic write and query",
			writePoints: []Point{
				{
					Metric:    "cpu_usage",
					Value:     75.5,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server1",
						"region": "us-west",
					},
				},
				{
					Metric:    "cpu_usage",
					Value:     82.3,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server2",
						"region": "us-east",
					},
				},
			},
			queryStr: fmt.Sprintf("SELECT avg FROM cpu_usage GROUP BY region BETWEEN %d:%d", hourAgo, now),
			verify: func(t *testing.T, results []QueryResult) {
				if len(results) != 1 {
					t.Fatalf("expected 1 result, got %d", len(results))
				}

				avgResult := results[0]
				if avgResult.Aggregate != "AVG" {
					t.Errorf("expected AVG aggregate, got %s", avgResult.Aggregate)
				}

				// Check that we have results for both regions
				if len(avgResult.Result) != 2 {
					t.Errorf("expected 2 regions in result, got %d", len(avgResult.Result))
				}

				// Verify the averages are correct
				expectedValues := map[string]float64{
					"us-west": 75.5,
					"us-east": 82.3,
				}

				for region, expectedVal := range expectedValues {
					if val, ok := avgResult.Result[region]; !ok {
						t.Errorf("missing result for region %s", region)
					} else if val != expectedVal {
						t.Errorf("for region %s: expected %.2f, got %.2f", region, expectedVal, val)
					}
				}
			},
		},
		{
			name: "multi-metric write and query",
			writePoints: []Point{
				{
					Metric:    "memory_usage",
					Value:     1024,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server1",
						"region": "us-west",
					},
				},
				{
					Metric:    "memory_usage",
					Value:     2048,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server2",
						"region": "us-east",
					},
				},
			},
			queryStr: fmt.Sprintf("SELECT sum FROM memory_usage GROUP BY region BETWEEN %d:%d", hourAgo, now),
			verify: func(t *testing.T, results []QueryResult) {
				if len(results) != 1 {
					t.Fatalf("expected 1 result, got %d", len(results))
				}

				sumResult := results[0]
				if sumResult.Aggregate != "SUM" {
					t.Errorf("expected SUM aggregate, got %s", sumResult.Aggregate)
				}

				expectedSums := map[string]float64{
					"us-west": 1024,
					"us-east": 2048,
				}

				for region, expectedSum := range expectedSums {
					if val, ok := sumResult.Result[region]; !ok {
						t.Errorf("missing result for region %s", region)
					} else if val != expectedSum {
						t.Errorf("for region %s: expected %.2f, got %.2f", region, expectedSum, val)
					}
				}
			},
		},
		{
			name: "write with multiple aggregations",
			writePoints: []Point{
				{
					Metric:    "disk_usage",
					Value:     50.5,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server1",
						"region": "us-west",
						"disk":   "sda",
					},
				},
				{
					Metric:    "disk_usage",
					Value:     75.5,
					Timestamp: now,
					Tags: map[string]string{
						"host":   "server1",
						"region": "us-west",
						"disk":   "sdb",
					},
				},
			},
			queryStr: fmt.Sprintf("SELECT avg, max FROM disk_usage GROUP BY region BETWEEN %d:%d", hourAgo, now),
			verify: func(t *testing.T, results []QueryResult) {
				if len(results) != 2 {
					t.Fatalf("expected 2 results (AVG and MAX), got %d", len(results))
				}

				// Verify average result
				for _, result := range results {
					switch result.Aggregate {
					case "AVG":
						avgVal, ok := result.Result["us-west"]
						if !ok {
							t.Error("missing us-west region in avg result")
						}
						expectedAvg := (50.5 + 75.5) / 2
						if avgVal != expectedAvg {
							t.Errorf("expected avg %.2f, got %.2f", expectedAvg, avgVal)
						}
					case "MAX":
						maxVal, ok := result.Result["us-west"]
						if !ok {
							t.Error("missing us-west region in max result")
						}
						if maxVal != 75.5 {
							t.Errorf("expected max 75.5, got %.2f", maxVal)
						}
					default:
						t.Errorf("unexpected aggregate type: %s", result.Aggregate)
					}
				}
			},
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write points
			err := client.Write(tt.writePoints)
			if err != nil {
				t.Fatalf("failed to write points: %v", err)
			}

			// Give some time for replication
			time.Sleep(time.Second)

			// Query and verify
			results, err := client.Query(tt.queryStr)
			if err != nil {
				t.Fatalf("failed to execute query: %v", err)
			}

			tt.verify(t, results)
		})
	}

	// Cleanup
	for i := range nodes {
		if !nodes[i].isClosed {
			nodes[i].service.Close()
			nodes[i].db.Close()
			nodes[i].isClosed = true
		}
	}
}
