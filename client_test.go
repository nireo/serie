package serie

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

type testNode struct {
	addr     string
	db       *TSMTree
	service  *HttpService
	isClosed bool
}

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

	defer os.RemoveAll("./testdata")

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

	distributor := NewDistributor(10, 3)
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

	for _, member := range distributor.HashRing.GetMembers() {
		fmt.Println(member)
	}

	now := time.Now().Unix()
	points := []Point{
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
	}

	client := &Client{distributor: distributor}
	err := client.Write(points)
	if err != nil {
		t.Fatalf("error writing points: %s", err)
	}
	time.Sleep(500 * time.Millisecond)

	querystr := fmt.Sprintf("SELECT avg FROM cpu_usage GROUP BY region BETWEEN %d:%d", now-3600, now)
	read, err := client.Query(querystr)
	if err != nil {
		t.Fatalf("error reading %s", err)
	}
	fmt.Fprintf(os.Stderr, "query result: %+v", read[0])
	if len(read) != 1 {
		t.Fatalf("wrong amount of query points: %d", len(read))
	}

	qr := read[0]
	if qr.Aggregate != "avg" {
		t.Fatalf("wrong aggregate got: %s", qr.Aggregate)
	}

	if len(qr.Result) != 2 {
		t.Fatalf("wrong amount of results: %d", len(qr.Result))
	}

	if qr.Result["us-east"] != 82.3 || qr.Result["us-west"] != 75.5 {
		t.Fatalf("wrong results: %+v", qr.Result)
	}

	// ensure that replication is done
	replicationNodes, err := distributor.ChooseNodes("cpu_usage")
	if err != nil {
		t.Fatalf("error getting nodes")
	}

	for _, rnode := range replicationNodes {
		res := helpReqNode(t, rnode.String(), querystr)
		if len(res[0].Result) != 2 {
			t.Fatalf("wrong amount of results: %d", len(res[0].Result))
		}

		if res[0].Result["us-east"] != 82.3 || res[0].Result["us-west"] != 75.5 {
			t.Fatalf("wrong results: %+v", res[0].Result)
		}
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

func helpReqNode(t *testing.T, addr, query string) []QueryResult {
	t.Helper()

	urlValues := url.Values{}
	urlValues.Add("query", query)
	url := fmt.Sprintf("http://%s/query", addr)

	fullUrl := fmt.Sprintf("%s?%s", url, urlValues.Encode())

	// if we error here we have some other problems so just quit
	req, err := http.NewRequest(http.MethodGet, fullUrl, nil)
	if err != nil {
		t.Fatalf("cannot make request: %s", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("cannot do request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code wasn't ok got: %d", resp.StatusCode)
	}

	var queryResult []QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
		t.Fatalf("cannot decode body: %s", err)
	}

	return queryResult
}

func getFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

func generateNTestNodes(t *testing.T, count int) []testNode {
	testNodes := make([]testNode, count)
	for i := 0; i < count; i++ {
		port, err := getFreePort()
		if err != nil {
			t.Fatalf("failed to get free port: %s", err)
		}

		testNodes[i].addr = fmt.Sprintf("http://localhost:%d", port)
		fmt.Fprintf(os.Stderr, "%s\n", testNodes[i].addr)

		db, err := NewTSMTree(Config{
			MaxMemSize:    1024 * 1024, // 1MB for testing
			DataDir:       fmt.Sprintf("./testdata/node%d", i),
			FlushInterval: time.Second * 5,
		})
		if err != nil {
			t.Fatalf("failed to create TSMTree for node %d: %v", i, err)
		}
		testNodes[i].db = db

		service := NewHttpService(testNodes[i].addr, db)
		if err := service.Start(); err != nil {
			t.Fatalf("failed to start HTTP service for node %d: %v", i, err)
		}
		testNodes[i].service = service
	}

	return testNodes
}

func closeNodes(nodes []testNode) {
	for i := range nodes {
		if !nodes[i].isClosed {
			nodes[i].service.Close()
			nodes[i].db.Close()
			nodes[i].isClosed = true
		}
	}
}

func TestBulkWriteIntegration(t *testing.T) {
	nodes := generateNTestNodes(t, 6)
	defer closeNodes(nodes)
	defer os.RemoveAll("./testdata")

	// Setup distributor
	distributor := NewDistributor(10, 3)
	distributor.ClusterFilePath = "./testdata/cluster_bulk.json"
	distributor.HeartbeatTimeout = time.Second * 5

	// Add nodes to distributor
	for _, node := range nodes {
		err := distributor.AddNode(&Member{
			Name: fmt.Sprintf("node-%s", node.addr),
			Addr: node.addr,
		})
		if err != nil {
			t.Fatalf("failed to add node to distributor: %v", err)
		}
	}

	// Configuration for bulk data
	config := struct {
		numPoints     int
		numHosts      int
		numRegions    int
		timeSpanHours int
	}{
		numPoints:     10000, // Number of data points to generate
		numHosts:      10,    // Number of different hosts
		numRegions:    5,     // Number of different regions
		timeSpanHours: 24,    // Time span for the data in hours
	}

	// Generate test data points
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(config.timeSpanHours) * time.Hour)
	points := make([]Point, config.numPoints)

	metrics := []string{"cpu_usage", "memory_usage", "disk_usage", "network_in", "network_out"}
	regions := make([]string, config.numRegions)
	for i := 0; i < config.numRegions; i++ {
		regions[i] = fmt.Sprintf("region-%d", i)
	}

	for i := 0; i < config.numPoints; i++ {
		// Generate random timestamp within the time span
		timestamp := startTime.Add(time.Duration(float64(endTime.Sub(startTime)) * float64(i) / float64(config.numPoints)))

		// Select random metric, host, and region
		metric := metrics[i%len(metrics)]
		host := fmt.Sprintf("host-%d", i%config.numHosts)
		region := regions[i%len(regions)]

		// Generate semi-realistic value based on metric type
		var value float64
		switch metric {
		case "cpu_usage":
			value = 20 + 60*float64(i%100)/100 // Values between 20-80
		case "memory_usage":
			value = 40 + 50*float64(i%100)/100 // Values between 40-90
		case "disk_usage":
			value = 30 + 65*float64(i%100)/100 // Values between 30-95
		case "network_in", "network_out":
			value = float64(100 + (i % 900)) // Values between 100-1000
		}

		points[i] = Point{
			Metric:    metric,
			Value:     value,
			Timestamp: timestamp.Unix(),
			Tags: map[string]string{
				"host":   host,
				"region": region,
			},
		}
	}

	// Write points
	client := &Client{distributor: distributor}
	err := client.Write(points)
	if err != nil {
		t.Fatalf("error writing points: %s", err)
	}

	// Allow time for writes to complete
	time.Sleep(time.Second)

	// Verify data with different queries
	testCases := []struct {
		name    string
		metric  string
		groupBy string
	}{
		{"CPU by Region", "cpu_usage", "region"},
		{"Memory by Region", "memory_usage", "region"},
		{"Disk by Host", "disk_usage", "host"},
		{"Network In by Region", "network_in", "region"},
		{"Network Out by Host", "network_out", "host"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := fmt.Sprintf("SELECT avg FROM %s GROUP BY %s BETWEEN %d:%d",
				tc.metric,
				tc.groupBy,
				startTime.Unix(),
				endTime.Unix())

			// Query the data
			read, err := client.Query(query)
			if err != nil {
				t.Fatalf("error reading %s: %v", tc.name, err)
			}

			if len(read) == 0 {
				t.Fatalf("no results returned for %s", tc.name)
			}

			qr := read[0]

			// Verify we got results
			if len(qr.Result) == 0 {
				t.Errorf("no data returned for %s", tc.name)
			}

			// Verify each node has the data
			replicationNodes, err := distributor.ChooseNodes(tc.metric)
			if err != nil {
				t.Fatalf("error getting nodes: %v", err)
			}

			for _, rnode := range replicationNodes {
				res := helpReqNode(t, rnode.String(), query)
				if len(res) == 0 || len(res[0].Result) == 0 {
					t.Errorf("node %s missing data for %s", rnode, tc.name)
				}
			}
		})
	}

	// Print some statistics
	t.Logf("Successfully wrote and verified %d points across %d hosts in %d regions",
		config.numPoints, config.numHosts, config.numRegions)
}
