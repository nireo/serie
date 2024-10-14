package serie

import (
	"io"
	"os"
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

func TestTSMFileWriteRead(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "tsm")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	tsmFile := &TSMFile{
		file:     tmpfile,
		index:    make(map[string][]IndexEntry),
		writePos: 0,
	}

	key := "cpu.usage"
	points := []Point{
		{Timestamp: 1000, Value: 10.0,
			Tags: map[string]string{
				"tag1": "value2",
			},
		},
		{Timestamp: 2000, Value: 20.0,
			Tags: map[string]string{
				"tag1": "value3",
			},
		},
		{Timestamp: 3000, Value: 30.0,
			Tags: map[string]string{
				"tag1": "value6",
			},
		},
		{Timestamp: 4000, Value: 40.0,
			Tags: map[string]string{
				"tag2": "value1",
			},
		},
		{Timestamp: 5000, Value: 50.0,
			Tags: map[string]string{
				"tag1": "value3",
			},
		},
	}

	err = tsmFile.write(key, points)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	err = tsmFile.finalize()
	if err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}

	_, err = tsmFile.file.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	readPoints, err := tsmFile.read(key, 0, 6000)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(readPoints) != len(points) {
		t.Fatalf("Expected %d points, got %d", len(points), len(readPoints))
	}
	for i, p := range points {
		if p.Timestamp != readPoints[i].Timestamp || p.Value != readPoints[i].Value {
			t.Errorf("Point mismatch at index %d. Expected %v, got %v", i, p, readPoints[i])
		}
	}

	readPoints, err = tsmFile.read(key, 2500, 4500)
	if err != nil {
		t.Fatalf("Failed to read subset of data: %v", err)
	}
	if len(readPoints) != 2 {
		t.Fatalf("Expected 2 points in subset, got %d", len(readPoints))
	}
	if readPoints[0].Timestamp != 3000 || readPoints[1].Timestamp != 4000 {
		t.Errorf("Unexpected timestamps in subset. Got %v and %v", readPoints[0].Timestamp, readPoints[1].Timestamp)
	}

	if readPoints[0].Tags["tag1"] != "value6" || readPoints[1].Tags["tag2"] != "value1" {
		t.Errorf("Unexpected tag values. Got: %v and %v", readPoints[0].Tags["tag1"], readPoints[0].Tags["tag2"])
	}

	readPoints, err = tsmFile.read("non.existent.key", 0, 6000)
	if err != nil {
		t.Fatalf("Failed to read non-existent key: %v", err)
	}
	if len(readPoints) != 0 {
		t.Errorf("Expected 0 points for non-existent key, got %d", len(readPoints))
	}
}

func TestTSMFileWriteReadMultipleKeys(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "tsm")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	tsmFile := &TSMFile{
		file:     tmpfile,
		index:    make(map[string][]IndexEntry),
		writePos: 0,
	}

	data := map[string][]Point{
		"cpu.usage": {
			{Timestamp: 1000, Value: 10.0},
			{Timestamp: 2000, Value: 20.0},
		},
		"mem.usage": {
			{Timestamp: 1500, Value: 15.0},
			{Timestamp: 2500, Value: 25.0},
		},
	}

	for key, points := range data {
		err = tsmFile.write(key, points)
		if err != nil {
			t.Fatalf("Failed to write data for key %s: %v", key, err)
		}
	}

	err = tsmFile.finalize()
	if err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}

	_, err = tsmFile.file.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	for key, expectedPoints := range data {
		readPoints, err := tsmFile.read(key, 0, 3000)
		if err != nil {
			t.Fatalf("Failed to read data for key %s: %v", key, err)
		}

		if len(readPoints) != len(expectedPoints) {
			t.Fatalf("For key %s: expected %d points, got %d", key, len(expectedPoints), len(readPoints))
		}

		for i, p := range expectedPoints {
			if p.Timestamp != readPoints[i].Timestamp || p.Value != readPoints[i].Value {
				t.Errorf("For key %s: point mismatch at index %d. Expected %v, got %v", key, i, p, readPoints[i])
			}
		}
	}
}
