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

func TestHeartbeat(t *testing.T) {
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
