package serie

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDistributor_Dump(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "distributor-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name    string
		members []*Member
		wantErr bool
	}{
		{
			name: "successful dump with multiple members",
			members: []*Member{
				{Name: "node1", Addr: "localhost:8081"},
				{Name: "node2", Addr: "localhost:8082"},
				{Name: "node3", Addr: "localhost:8083"},
			},
			wantErr: false,
		},
		{
			name:    "successful dump with empty members",
			members: []*Member{},
			wantErr: false,
		},
		{
			name: "single member dump",
			members: []*Member{
				{Name: "node1", Addr: "localhost:8081"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDistributor(10, 3)
			d.Members = tt.members

			d.ClusterFilePath = filepath.Join(tmpDir, tt.name+".json")
			err := d.Dump()

			if (err != nil) != tt.wantErr {
				t.Errorf("Dump() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				file, err := os.Open(d.ClusterFilePath)
				if err != nil {
					t.Errorf("failed to open dumped file: %v", err)
					return
				}
				defer file.Close()

				var loadedMembers []Member
				decoder := json.NewDecoder(file)
				if err := decoder.Decode(&loadedMembers); err != nil {
					t.Errorf("failed to decode dumped file: %v", err)
					return
				}

				if len(loadedMembers) != len(tt.members) {
					t.Errorf("loaded members length = %v, want %v", len(loadedMembers), len(tt.members))
					return
				}

				for i, member := range tt.members {
					if member.Name != loadedMembers[i].Name || member.Addr != loadedMembers[i].Addr {
						t.Errorf("member %d mismatch: got %v, want %v", i, loadedMembers[i], member)
					}
				}
			}
		})
	}
}

type mockServer struct {
	distributor *Distributor
	listener    net.Listener
	httpServer  *http.Server
	wg          sync.WaitGroup
}

var rpcOnce sync.Once

func startMockServer(t *testing.T, tmpDir, addr string) *mockServer {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start listener on %s: %v", addr, err)
	}

	server := &mockServer{
		distributor: NewDistributor(10, 3),
		listener:    listener,
	}
	server.distributor.ClusterFilePath = filepath.Join(tmpDir, fmt.Sprintf("cluster_%s.json", addr))

	rpcOnce.Do(func() {
		rpc.RegisterName("Distributor", server.distributor)
		rpc.HandleHTTP()
	})

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpc.DefaultServer)
	mux.Handle(rpc.DefaultDebugPath, rpc.DefaultServer)
	server.httpServer = &http.Server{
		Handler: mux,
	}

	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		server.httpServer.Serve(listener)
	}()

	return server
}

func (s *mockServer) stop() {
	s.httpServer.Close()
	s.listener.Close()
	s.wg.Wait()
}

func TestHeartbeatFull(t *testing.T) {
	servers := make([]*mockServer, 3)
	members := make([]*Member, 3)

	tmpDir, err := os.MkdirTemp("", "cluster-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 50000+i)
		servers[i] = startMockServer(t, tmpDir, addr)
		members[i] = &Member{
			Addr: addr,
			Name: fmt.Sprintf("node%d", i),
		}
		defer servers[i].stop()
	}

	mainDist := servers[0].distributor
	mainDist.Members = members

	t.Run("Basic Heartbeat", func(t *testing.T) {
		manager := mainDist.InitializeHeartbeat(2 * time.Second)
		defer manager.Stop()

		time.Sleep(3 * time.Second)

		for _, member := range members {
			if !mainDist.IsNodeHealthy(member.Addr) {
				t.Errorf("Node %s should be healthy", member.Addr)
			}
		}
	})

	t.Run("Dead Node Detection", func(t *testing.T) {
		manager := mainDist.InitializeHeartbeat(2 * time.Second)
		defer manager.Stop()

		time.Sleep(1 * time.Second)
		servers[1].stop()
		time.Sleep(3 * time.Second)

		found := false
		mainDist.mu.RLock()
		for _, member := range mainDist.Members {
			if member.Addr == members[1].Addr {
				found = true
				break
			}
		}
		mainDist.mu.RUnlock()

		if found {
			t.Error("Dead node should have been removed from members list")
		}
	})

	t.Run("Node Recovery", func(t *testing.T) {
		manager := mainDist.InitializeHeartbeat(2 * time.Second)
		defer manager.Stop()

		recoveredNode := &Member{
			Addr: "127.0.0.1:50003",
			Name: "recovered",
		}

		recoveredServer := startMockServer(t, tmpDir, recoveredNode.Addr)
		defer recoveredServer.stop()

		err := mainDist.AddNode(recoveredNode)
		if err != nil {
			t.Fatalf("Failed to add recovered node: %v", err)
		}

		time.Sleep(3 * time.Second)
		if !mainDist.IsNodeHealthy(recoveredNode.Addr) {
			t.Error("Recovered node should be healthy")
		}
	})

	t.Run("Network Partition", func(t *testing.T) {
		manager := mainDist.InitializeHeartbeat(2 * time.Second)
		defer manager.Stop()

		for i := 1; i < len(servers); i++ {
			servers[i].stop()
		}

		time.Sleep(3 * time.Second)

		mainDist.mu.RLock()
		activeNodes := len(mainDist.Members)
		mainDist.mu.RUnlock()

		if activeNodes != 1 {
			t.Errorf("Expected 1 active node during partition, got %d", activeNodes)
		}
	})

	t.Run("Client Cache Management", func(t *testing.T) {
		manager := mainDist.InitializeHeartbeat(2 * time.Second)
		defer manager.Stop()

		time.Sleep(1 * time.Second)

		manager.mu.RLock()
		initialClients := len(manager.clients)
		manager.mu.RUnlock()

		if initialClients == 0 {
			t.Error("Client cache should not be empty")
		}

		for i := 1; i < len(servers); i++ {
			servers[i].stop()
		}

		time.Sleep(3 * time.Second)

		manager.mu.RLock()
		finalClients := len(manager.clients)
		manager.mu.RUnlock()

		if finalClients >= initialClients {
			t.Error("Client cache should have been cleaned up")
		}
	})
}

func TestDistributorBasics(t *testing.T) {
	d := NewDistributor(271, 2)
	if d == nil {
		t.Fatal("NewDistributor returned nil")
	}

	if d.ReplicationCount != 2 {
		t.Errorf("Expected ReplicationCount to be 2, got %d", d.ReplicationCount)
	}

	if d.HashRing == nil {
		t.Error("HashRing should not be nil")
	}
}

func TestAddNode(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "cluster*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	d := NewDistributor(271, 2)
	d.ClusterFilePath = tmpfile.Name()

	member := &Member{
		Addr: "localhost:8001",
		Name: "node1",
	}

	if err := d.AddNode(member); err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	if len(d.Members) != 1 {
		t.Errorf("Expected 1 member, got %d", len(d.Members))
	}

	// Verify the node was written to the file
	data, err := os.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	var members []*Member
	if err := json.Unmarshal(data, &members); err != nil {
		t.Fatal(err)
	}

	if len(members) != 1 {
		t.Errorf("Expected 1 member in file, got %d", len(members))
	}
}

func TestChooseNodes(t *testing.T) {
	d := NewDistributor(271, 2)

	members := []*Member{
		{Addr: "localhost:8001", Name: "node1"},
		{Addr: "localhost:8002", Name: "node2"},
		{Addr: "localhost:8003", Name: "node3"},
	}

	for _, m := range members {
		d.HashRing.Add(m)
	}

	nodes, err := d.ChooseNodes("test.metric")
	if err != nil {
		t.Fatalf("Failed to choose nodes: %v", err)
	}

	if len(nodes) != d.ReplicationCount {
		t.Errorf("Expected %d nodes, got %d", d.ReplicationCount, len(nodes))
	}
}

func TestHeartbeat(t *testing.T) {
	d := NewDistributor(271, 2)
	d.heartbeats = make(map[string]time.Time)

	args := &HeartbeatArgs{From: "localhost:8001"}
	reply := &HeartbeatReply{}

	before := time.Now()
	err := d.Heartbeat(args, reply)
	after := time.Now()

	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	timestamp, exists := d.heartbeats[args.From]
	if !exists {
		t.Fatal("Heartbeat was not recorded")
	}

	if timestamp.Before(before) || timestamp.After(after) {
		t.Error("Heartbeat timestamp is outside expected range")
	}
}

func TestNodeHealth(t *testing.T) {
	d := NewDistributor(271, 2)
	d.HeartbeatTimeout = 1 * time.Second
	d.heartbeats = make(map[string]time.Time)

	addr := "localhost:8001"
	d.heartbeats[addr] = time.Now()

	if !d.IsNodeHealthy(addr) {
		t.Error("Node should be healthy")
	}

	d.heartbeats[addr] = time.Now().Add(-2 * time.Second)
	if d.IsNodeHealthy(addr) {
		t.Error("Node should be unhealthy")
	}
}

func TestHeartbeatManager(t *testing.T) {
	// Create temporary files for both distributors
	tmpFile1, err := os.CreateTemp("", "cluster1*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile1.Name())

	tmpFile2, err := os.CreateTemp("", "cluster2*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile2.Name())

	// Create two distributors to simulate a cluster
	d1 := NewDistributor(271, 2)
	d2 := NewDistributor(271, 2)

	// Set the cluster file paths
	d1.ClusterFilePath = tmpFile1.Name()
	d2.ClusterFilePath = tmpFile2.Name()

	// Setup separate RPC servers for each distributor
	server1 := rpc.NewServer()
	server2 := rpc.NewServer()

	if err := server1.Register(d1); err != nil {
		t.Fatalf("Failed to register d1: %v", err)
	}
	if err := server2.Register(d2); err != nil {
		t.Fatalf("Failed to register d2: %v", err)
	}

	// Start servers
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener1.Close()

	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener2.Close()

	// Create separate HTTP servers
	mux1 := http.NewServeMux()
	mux2 := http.NewServeMux()

	mux1.Handle(rpc.DefaultRPCPath, server1)
	mux2.Handle(rpc.DefaultRPCPath, server2)

	server1Http := &http.Server{Handler: mux1}
	server2Http := &http.Server{Handler: mux2}

	go server1Http.Serve(listener1)
	go server2Http.Serve(listener2)

	// Add members
	member1 := &Member{
		Addr: listener1.Addr().String(),
		Name: "node1",
	}
	member2 := &Member{
		Addr: listener2.Addr().String(),
		Name: "node2",
	}

	// Add nodes properly using AddNode
	if err := d1.AddNode(member1); err != nil {
		t.Fatal(err)
	}
	if err := d1.AddNode(member2); err != nil {
		t.Fatal(err)
	}
	if err := d2.AddNode(member1); err != nil {
		t.Fatal(err)
	}
	if err := d2.AddNode(member2); err != nil {
		t.Fatal(err)
	}

	// Initialize heartbeat managers with shorter timeout for testing
	heartbeatTimeout := 500 * time.Millisecond
	manager1 := d1.InitializeHeartbeat(heartbeatTimeout)
	manager2 := d2.InitializeHeartbeat(heartbeatTimeout)

	defer manager1.Stop()
	defer manager2.Stop()
	defer server1Http.Close()
	defer server2Http.Close()

	// Helper function to wait for node health with timeout
	waitForHealthy := func(d *Distributor, addr string, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if d.IsNodeHealthy(addr) {
				return true
			}
			time.Sleep(50 * time.Millisecond)
		}
		return false
	}

	// Wait and verify with more detailed error messages
	timeout := 5 * time.Second
	if !waitForHealthy(d1, member2.Addr, timeout) {
		t.Errorf("Node 2 (%s) not healthy according to node 1 after %v. Heartbeats: %+v",
			member2.Addr, timeout, d1.heartbeats)
	}

	if !waitForHealthy(d2, member1.Addr, timeout) {
		t.Errorf("Node 1 (%s) not healthy according to node 2 after %v. Heartbeats: %+v",
			member1.Addr, timeout, d2.heartbeats)
	}

	// Additional verification
	healthyNodes1 := d1.GetHealthyNodes()
	healthyNodes2 := d2.GetHealthyNodes()

	if len(healthyNodes1) != 2 {
		t.Errorf("Node 1 sees %d healthy nodes, expected 2. Nodes: %+v",
			len(healthyNodes1), healthyNodes1)
	}

	if len(healthyNodes2) != 2 {
		t.Errorf("Node 2 sees %d healthy nodes, expected 2. Nodes: %+v",
			len(healthyNodes2), healthyNodes2)
	}
}
