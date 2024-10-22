package serie

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type hasher struct{}

type Member struct {
	Addr string `json:"addr"`
	Name string `json:"name"`
}

func (m *Member) String() string {
	return m.Addr
}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// Distributor handles distributing data to different cluster nodes. The main idea is to use consistent hashing
// based on the metrics. A replication count can be configured such that the amount of nodes the data is redirected
// to. The idea behind the distributor is mainly to return a list of nodes that should be written to. It doesn't
// direct points into some certain place it just takes care consensus between nodes on which should handle which
// metric.
type Distributor struct {
	ClusterFilePath  string // ClusterFile is a path to the file that contains the members of a cluster.
	Members          []*Member
	HashRing         *consistent.Consistent
	HeartbeatTimeout time.Duration
	ReplicationCount int
	heartbeats       map[string]time.Time // address -> time when nodes responded to heartbeat.
	mu               sync.RWMutex
}

func NewDistributor(partitionCount, replicationFactor int) *Distributor {
	conf := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}

	return &Distributor{
		HashRing:         consistent.New(nil, conf),
		ReplicationCount: 2,
	}
}

type HeartbeatManager struct {
	distributor *Distributor
	stopChan    chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex
	clients     map[string]*rpc.Client
}

func (d *Distributor) InitializeHeartbeat(heartbeatTimeout time.Duration) *HeartbeatManager {
	if d.heartbeats == nil {
		d.heartbeats = make(map[string]time.Time)
	}
	d.HeartbeatTimeout = heartbeatTimeout

	manager := &HeartbeatManager{
		distributor: d,
		stopChan:    make(chan struct{}),
		clients:     make(map[string]*rpc.Client),
	}

	manager.wg.Add(1)
	go manager.heartbeatLoop()

	return manager
}

func (h *HeartbeatManager) heartbeatLoop() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.distributor.HeartbeatTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopChan:
			return
		case <-ticker.C:
			h.checkAndSendHeartbeats()
		}
	}
}

func (h *HeartbeatManager) checkAndSendHeartbeats() {
	h.distributor.mu.Lock()
	now := time.Now()
	deadNodes := make([]*Member, 0)
	members := make([]*Member, len(h.distributor.Members))
	copy(members, h.distributor.Members)
	thisAddr := h.distributor.Members[0].Addr

	for _, member := range members {
		lastHeartbeat, exists := h.distributor.heartbeats[member.Addr]
		if !exists || now.Sub(lastHeartbeat) > h.distributor.HeartbeatTimeout {
			deadNodes = append(deadNodes, member)
		}
	}

	for _, deadNode := range deadNodes {
		h.distributor.removeNode(deadNode)
		h.mu.Lock()
		if client, exists := h.clients[deadNode.Addr]; exists {
			client.Close()
			delete(h.clients, deadNode.Addr)
		}
		h.mu.Unlock()
	}
	h.distributor.mu.Unlock()

	var wg sync.WaitGroup
	for _, member := range members {
		if member.Addr == thisAddr || h.isNodeDead(member, deadNodes) {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := h.sendHeartbeat(addr, thisAddr); err != nil {
				fmt.Printf("Failed to send heartbeat to %s: %v\n", addr, err)
			}
		}(member.Addr)
	}
	wg.Wait()
}

func (h *HeartbeatManager) isNodeDead(node *Member, deadNodes []*Member) bool {
	for _, deadNode := range deadNodes {
		if deadNode.Addr == node.Addr {
			return true
		}
	}
	return false
}

func (h *HeartbeatManager) sendHeartbeat(targetAddr, fromAddr string) error {
	h.mu.Lock()
	client, exists := h.clients[targetAddr]
	if !exists || client == nil {
		var err error
		client, err = rpc.DialHTTP("tcp", targetAddr)
		if err != nil {
			h.mu.Unlock()
			return fmt.Errorf("failed to connect to %s: %v", targetAddr, err)
		}
		h.clients[targetAddr] = client
	}
	h.mu.Unlock()

	args := &HeartbeatArgs{
		From: fromAddr,
	}
	reply := &HeartbeatReply{}

	err := client.Call("Distributor.Heartbeat", args, reply)
	if err != nil {
		h.mu.Lock()
		delete(h.clients, targetAddr)
		h.mu.Unlock()
		return err
	}

	return nil
}

func (h *HeartbeatManager) Stop() {
	close(h.stopChan)
	h.wg.Wait()

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, client := range h.clients {
		if client != nil {
			client.Close()
		}
	}
}

func (d *Distributor) removeNode(member *Member) {
	d.HashRing.Remove(member.String())
	newMembers := make([]*Member, 0, len(d.Members)-1)
	for _, m := range d.Members {
		if m.Addr != member.Addr {
			newMembers = append(newMembers, m)
		}
	}
	d.Members = newMembers

	delete(d.heartbeats, member.Addr)
	if err := d.Dump(); err != nil {
		fmt.Printf("Failed to dump configuration after removing node %s: %v\n", member.Addr, err)
	}
}

// Dump dumps the member configuration to a file on disk.
func (d *Distributor) Dump() error {
	file, err := os.Create(d.ClusterFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ")

	err = json.NewEncoder(file).Encode(d.Members)
	if err != nil {
		return err
	}

	return nil
}

// AddNode adds a given node to the hash ring and also dumps the contents of the
// members to the specified cluster configuration file.
func (d *Distributor) AddNode(member *Member) error {
	d.HashRing.Add(member)
	d.Members = append(d.Members, member)

	// This operation is infrequent enough to not worry about so we can do an expensive
	// operation here.
	return d.Dump()
}

// ChooseNodes distributes the nodes for a given metric. It chooses the d.ReplicationCount amount of
// nodes. The nodes are then returned which should be written to.
func (d *Distributor) ChooseNodes(metric string) ([]consistent.Member, error) {
	nodes, err := d.HashRing.GetClosestN([]byte(metric), d.ReplicationCount)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

type HeartbeatArgs struct {
	// The address of the node sending this request. this is done to reduce the amount of requests.
	// Since if a node has sent this node a request that means that we don't have to request a heartbeat
	// from that node since this node must know it's alive.
	From string
}

type HeartbeatReply struct{}

func (d *Distributor) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// This method only logs the from id as being alive.
	d.heartbeats[args.From] = time.Now()
	return nil
}
