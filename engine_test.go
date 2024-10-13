package serie

import (
	"sync"
	"testing"
	"time"
)

func TestTSMTreeWrite(t *testing.T) {
	tree := NewTSMTree("/tmp/tsmtest", 100)

	err := tree.Write("cpu.usage", 1000, 50.0)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if len(tree.mem.data["cpu.usage"]) != 1 {
		t.Errorf("Expected 1 data point in memTable, got %d", len(tree.mem.data["cpu.usage"]))
	}

	for i := 0; i < 10; i++ {
		err := tree.Write("cpu.usage", int64(2000+i), float64(50+i))
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	if len(tree.immutable) != 1 {
		t.Errorf("Expected 1 immutable, got %d", len(tree.immutable))
	}
}

func TestTSMTreeRead(t *testing.T) {
	tree := NewTSMTree("/tmp/tsmtest", 1000)
	testData := []struct {
		key       string
		timestamp int64
		value     float64
	}{
		{"cpu.usage", 1000, 50.0},
		{"cpu.usage", 2000, 60.0},
		{"cpu.usage", 3000, 70.0},
		{"mem.usage", 1500, 80.0},
		{"mem.usage", 2500, 90.0},
	}

	for _, d := range testData {
		err := tree.Write(d.key, d.timestamp, d.value)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	results, err := tree.Read("cpu.usage", 1500, 2500)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].Timestamp != 2000 || results[0].Value != 60.0 {
		t.Errorf("Unexpected result: got %v, want {2000 60.0}", results[0])
	}
}

func TestTSMTreeReadFromImmutable(t *testing.T) {
	tree := NewTSMTree("/tmp/tsmtest", 50)

	for i := 0; i < 10; i++ {
		err := tree.Write("cpu.usage", int64(1000+i*100), float64(50+i))
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	results, err := tree.Read("cpu.usage", 1000, 2000)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}

	for i, result := range results {
		expectedTimestamp := int64(1000 + i*100)
		expectedValue := float64(50 + i)
		if result.Timestamp != expectedTimestamp || result.Value != expectedValue {
			t.Errorf("Unexpected result at index %d: got {%d %f}, want {%d %f}",
				i, result.Timestamp, result.Value, expectedTimestamp, expectedValue)
		}
	}
}

func TestTSMTreeConcurrency(t *testing.T) {
	tree := NewTSMTree("/tmp/tsmtest", 1000)
	concurrency := 10
	writesPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				timestamp := time.Now().UnixNano()
				err := tree.Write("cpu.usage", timestamp, float64(id*writesPerGoroutine+j))
				if err != nil {
					t.Errorf("Failed to write data: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	totalWrites := concurrency * writesPerGoroutine
	results, err := tree.Read("cpu.usage", 0, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(results) != totalWrites {
		t.Errorf("Expected %d total writes, got %d", totalWrites, len(results))
	}
}
