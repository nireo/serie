package serie

import (
	"encoding/json"
	"os"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type hasher struct{}

type Member struct {
	Addr string `json:"addr"`
	Name string `json:"name"`
}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// Distributor handles distributing data to different cluster nodes. The main idea is to use consistent hashing
// based on the metrics. A replication count can be configured such that the
type Distributor struct {
	ClusterFilePath string // ClusterFile is a path to the file that contains the members of a cluster.
	Members         []Member
	HashRing        *consistent.Consistent
}

func NewDistributor(partitionCount, replicationFactor int) *Distributor {
	conf := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}

	return &Distributor{
		HashRing: consistent.New(nil, conf),
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
