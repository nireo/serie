package serie

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
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
