package serie

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDistributor_Dump(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "distributor-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name    string
		members []Member
		wantErr bool
	}{
		{
			name: "successful dump with multiple members",
			members: []Member{
				{Name: "node1", Addr: "localhost:8081"},
				{Name: "node2", Addr: "localhost:8082"},
				{Name: "node3", Addr: "localhost:8083"},
			},
			wantErr: false,
		},
		{
			name:    "successful dump with empty members",
			members: []Member{},
			wantErr: false,
		},
		{
			name: "single member dump",
			members: []Member{
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
