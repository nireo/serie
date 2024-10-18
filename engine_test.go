package serie

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func createTestTreeInTmpDir(t *testing.T, maxMemSize int) (*TSMTree, func()) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "serie-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// low size to force multiple immutable files
	tree, err := NewTSMTree(Config{
		DataDir:       tempDir,
		MaxMemSize:    maxMemSize,
		FlushInterval: 1 * time.Minute, // ensure that no flushes happen during this test.
	})
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	return tree, func() {
		tree.Close()
		os.RemoveAll(tempDir)
	}
}

func createTestTsmFile(t *testing.T) (*TSMFile, func()) {
	t.Helper()

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

	return tsmFile, func() {
		tsmFile.file.Close()
		os.Remove(tmpfile.Name())
	}
}

func TestTSMTreeWrite(t *testing.T) {
	tree, cleanup := createTestTreeInTmpDir(t, 100)
	defer cleanup()

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
	tree, cleanup := createTestTreeInTmpDir(t, 100)
	defer cleanup()

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
	tree, cleanup := createTestTreeInTmpDir(t, 50)
	defer cleanup()

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
	tree, cleanup := createTestTreeInTmpDir(t, 1000)
	defer cleanup()

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
	tsmFile, cleanup := createTestTsmFile(t)
	defer cleanup()

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

	err := tsmFile.write(key, points)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	err = tsmFile.file.Sync()
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
	tsmFile, cleanup := createTestTsmFile(t)
	defer cleanup()

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
		err := tsmFile.write(key, points)
		if err != nil {
			t.Fatalf("Failed to write data for key %s: %v", key, err)
		}
	}

	err := tsmFile.file.Sync()
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

func TestTSMFileEncodeDecodeIndex(t *testing.T) {
	tests := []struct {
		name  string
		index map[string][]IndexEntry
	}{
		{
			name:  "Empty index",
			index: map[string][]IndexEntry{},
		},
		{
			name: "Single metric, single entry",
			index: map[string][]IndexEntry{
				"cpu": {
					{MinTime: 100, MaxTime: 200, Offset: 0, Size: 100},
				},
			},
		},
		{
			name: "Multiple metrics, multiple entries",
			index: map[string][]IndexEntry{
				"cpu": {
					{MinTime: 100, MaxTime: 200, Offset: 0, Size: 100},
					{MinTime: 300, MaxTime: 400, Offset: 100, Size: 150},
				},
				"memory": {
					{MinTime: 150, MaxTime: 250, Offset: 250, Size: 120},
					{MinTime: 350, MaxTime: 450, Offset: 370, Size: 130},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tsmFile := &TSMFile{index: tt.index}

			encoded, err := tsmFile.encodeIndex()
			if err != nil {
				t.Fatalf("Failed to encode index: %v", err)
			}

			decodedTSMFile := &TSMFile{}

			err = decodedTSMFile.decodeIndex(encoded)
			if err != nil {
				t.Fatalf("Failed to decode index: %v", err)
			}

			if !reflect.DeepEqual(tt.index, decodedTSMFile.index) {
				t.Errorf("Decoded index does not match original index.\nOriginal: %+v\nDecoded: %+v", tt.index, decodedTSMFile.index)
			}
		})
	}
}

func generateTestPoints(metric string, minTimestamp int64, maxTimestamp int64, increment int64) []Point {
	var points []Point

	for ts := minTimestamp; ts <= maxTimestamp; ts += increment {
		points = append(points, Point{
			Metric:    metric,
			Timestamp: ts,
			Value:     rand.Float64(),
		})
	}

	return points
}

func TestParseDataDir(t *testing.T) {
	tree, cleanup := createTestTreeInTmpDir(t, 256)
	defer cleanup()

	err := tree.WriteBatch(generateTestPoints("parse-dir", 1000, 2000, 10))
	if err != nil {
		t.Fatalf("failed to write points: %v", err)
	}

	if err := tree.Flush(); err != nil {
		t.Fatalf("error flushing tree: %v", err)
	}

	expectedFileCount := len(tree.files)
	expectedIndexEntriesForPath := make(map[string][]IndexEntry)
	for _, f := range tree.files {
		expectedIndexEntriesForPath[f.path] = f.index["parse-dir"]
	}

	if err := tree.Close(); err != nil {
		t.Fatalf("failed to close tsm tree: %v", err)
	}

	tree2, err := NewTSMTree(Config{
		DataDir:       tree.dataDir,
		MaxMemSize:    256,
		FlushInterval: 1 * time.Minute, // ensure that no flushes happen during this test.
	})
	if err != nil {
		t.Fatalf("error creating new tree: %v", err)
	}

	if err := tree2.parseDataDir(); err != nil {
		t.Fatalf("error parsing data dir: %v", err)
	}

	if len(tree2.files) != expectedFileCount {
		t.Fatalf("parsed wrong amount of files, got: %v | expected: %v", len(tree2.files), expectedFileCount)
	}

	gotIndexEntriesForPath := make(map[string][]IndexEntry)
	for _, f := range tree2.files {
		gotIndexEntriesForPath[f.path] = f.index["parse-dir"]
	}

	fmt.Println(gotIndexEntriesForPath)

	if !reflect.DeepEqual(gotIndexEntriesForPath, expectedIndexEntriesForPath) {
		t.Fatalf("parsed file indecies differ\n\tgot: %+v\n\t want: %+v", gotIndexEntriesForPath, expectedFileCount)
	}
}

func TestTSMTreeGroupBy(t *testing.T) {
	tree, cleanup := createTestTreeInTmpDir(t, 1000)
	defer cleanup()

	testData := []Point{
		{Metric: "cpu.usage", Timestamp: 1000, Value: 50.0, Tags: map[string]string{"host": "server1", "dc": "us-west"}},
		{Metric: "cpu.usage", Timestamp: 2000, Value: 60.0, Tags: map[string]string{"host": "server1", "dc": "us-west"}},
		{Metric: "cpu.usage", Timestamp: 3000, Value: 70.0, Tags: map[string]string{"host": "server2", "dc": "us-east"}},
		{Metric: "cpu.usage", Timestamp: 4000, Value: 80.0, Tags: map[string]string{"host": "server2", "dc": "us-east"}},
		{Metric: "cpu.usage", Timestamp: 5000, Value: 90.0, Tags: map[string]string{"host": "server3", "dc": "eu-central"}},
	}

	err := tree.WriteBatch(testData)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	tests := []struct {
		name              string
		tagName           string
		aggregateFunction string
		expectedResult    map[string]float64
		expectedErrorMsg  string
	}{
		{
			name:              "Sum by host",
			tagName:           "host",
			aggregateFunction: "sum",
			expectedResult: map[string]float64{
				"server1": 110.0,
				"server2": 150.0,
				"server3": 90.0,
			},
		},
		{
			name:              "Average by dc",
			tagName:           "dc",
			aggregateFunction: "avg",
			expectedResult: map[string]float64{
				"us-west":    55.0,
				"us-east":    75.0,
				"eu-central": 90.0,
			},
		},
		{
			name:              "Count by host",
			tagName:           "host",
			aggregateFunction: "count",
			expectedResult: map[string]float64{
				"server1": 2,
				"server2": 2,
				"server3": 1,
			},
		},
		{
			name:              "Min by dc",
			tagName:           "dc",
			aggregateFunction: "min",
			expectedResult: map[string]float64{
				"us-west":    50.0,
				"us-east":    70.0,
				"eu-central": 90.0,
			},
		},
		{
			name:              "Max by host",
			tagName:           "host",
			aggregateFunction: "max",
			expectedResult: map[string]float64{
				"server1": 60.0,
				"server2": 80.0,
				"server3": 90.0,
			},
		},
		{
			name:              "Invalid aggregate function",
			tagName:           "host",
			aggregateFunction: "invalid",
			expectedErrorMsg:  "unrecognized function",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tree.groupBy(tt.tagName, tt.aggregateFunction, "cpu.usage", 0, 6000)

			if tt.expectedErrorMsg != "" {
				if err == nil || err.Error() != tt.expectedErrorMsg {
					t.Errorf("Expected error with message '%s', got: %v", tt.expectedErrorMsg, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(result, tt.expectedResult) {
				t.Errorf("Unexpected result.\nExpected: %v\nGot: %v", tt.expectedResult, result)
			}
		})
	}

	// Test with non-existent tag
	t.Run("Non-existent tag", func(t *testing.T) {
		result, err := tree.groupBy("non_existent_tag", "sum", "cpu.usage", 0, 6000)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := map[string]float64{"<no_tag>": 350.0}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Unexpected result for non-existent tag.\nExpected: %v\nGot: %v", expected, result)
		}
	})

	// Test with empty time range
	t.Run("Empty time range", func(t *testing.T) {
		result, err := tree.groupBy("host", "sum", "cpu.usage", 6000, 7000)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("Expected empty result for empty time range, got: %v", result)
		}
	})
}
