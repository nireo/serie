package serie

import (
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

func createTestTreeInTmpDir(t *testing.T, maxMemSize int) (*TSMTree, func()) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "serie-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	tree, err := NewTSMTree(Config{
		DataDir:       tempDir,
		MaxMemSize:    maxMemSize,
		FlushInterval: 1 * time.Minute,
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
		file:        tmpfile,
		index:       make(map[uint32][]IndexEntry),
		bloomFilter: bloom.NewWithEstimates(1000, 0.01),
		writePos:    0,
	}

	return tsmFile, func() {
		tsmFile.file.Close()
		os.Remove(tmpfile.Name())
	}
}

const float64EqualityThreshold = 1e-9

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}

func TestBasicWriteSingleEntry(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 10024)
	defer cleanup()

	m := "hello"
	ts := time.Now().Unix()
	var tss []int64
	var vals []float64
	tags := map[string]string{
		"hello": "world",
		"test":  "field",
	}
	for i := int64(0); i < 100; i++ {
		val := rand.Float64()
		tss = append(tss, ts+i)
		vals = append(vals, val)

		err := db.Write(m, ts+i, val, tags)
		if err != nil {
			t.Fatalf("error writing to db %s", err)
		}
	}

	res, err := db.Read(m, tss[0], tss[len(tss)-1], tags)
	if err != nil {
		t.Fatalf("error reading values: %s", err)
	}

	for i, val := range vals {
		if !almostEqual(val, res.Values[i]) {
			t.Fatalf("values were not equal, got=%f | want=%f", res.Values[i], val)
		}

		if tss[i] != res.Timestamps[i] {
			t.Fatalf("timestamps were not equal, got=%d | want=%d", res.Timestamps[i], tss[i])
		}
	}
}

func TestTSMFile(t *testing.T) {
	tm := TagMap{
		12: 15,
		1:  2,
	}
	sd := NewSeriesData(10, tm, 100)

	ts := time.Now().Unix()
	for i := int64(0); i < 100; i++ {
		val := rand.Float64()
		sd.Timestamps = append(sd.Timestamps, ts+i)
		sd.Values = append(sd.Values, val)
	}

	file, cleanup := createTestTsmFile(t)
	defer cleanup()

	err := file.writeSeriesBlock(sd)
	if err != nil {
		t.Fatalf("error writing series block %s", err)
	}

	res, err := file.readSeriesBlock(10, tm.Hash(), sd.Timestamps[0], sd.Timestamps[len(sd.Timestamps)-1])
	if err != nil {
		t.Fatalf("error reading series block %s", err)
	}

	for i, ti := range res.Timestamps {
		if !almostEqual(res.Values[i], sd.Values[i]) {
			t.Fatalf("values were not equal, got=%f | want=%f", res.Values[i], sd.Values[i])
		}

		if ti != sd.Timestamps[i] {
			t.Fatalf("timestamps were not equal, got=%d | want=%d", res.Timestamps[i], ti)
		}
	}
}

func TestIndexFile(t *testing.T) {
	file, cleanup := createTestTsmFile(t)
	defer cleanup()

	expectedIndex := map[uint32][]IndexEntry{
		1: {{
			MinTime: 123,
			MaxTime: 1234,
			Offset:  190,
			Size:    12,
			TagHash: 12341234,
		}},
		2: {{
			MinTime: 123,
			MaxTime: 1234,
			Offset:  19,
			Size:    1,
			TagHash: 12341234,
		}},
		3: {{
			MinTime: 1,
			MaxTime: 124,
			Offset:  19,
			Size:    12,
			TagHash: 123414,
		}},
	}

	file.index = expectedIndex

	data, err := file.encodeIndex()
	if err != nil {
		t.Fatalf("failed to encode data %s", err)
	}

	file.index = nil
	err = file.decodeIndex(data)
	if err != nil {
		t.Fatalf("failed to decode data %s", err)
	}

	for key, val := range expectedIndex {
		got := file.index[key]
		if !reflect.DeepEqual(got, val) {
			t.Fatalf("values are not equal %+v %+v", got, val)
		}
	}
}

func TestWriteReadWithDifferentTags(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024*1024)
	defer cleanup()

	metric := "cpu_usage"
	baseTime := time.Now().Unix()

	testCases := []struct {
		tags   map[string]string
		values []float64
		times  []int64
	}{
		{
			tags:   map[string]string{"host": "server1", "region": "us-east"},
			values: []float64{10.5, 20.3, 15.7},
			times:  []int64{baseTime, baseTime + 10, baseTime + 20},
		},
		{
			tags:   map[string]string{"host": "server2", "region": "us-east"},
			values: []float64{5.2, 12.8, 18.9},
			times:  []int64{baseTime + 5, baseTime + 15, baseTime + 25},
		},
		{
			tags:   map[string]string{"host": "server1", "region": "us-west"},
			values: []float64{8.1, 14.6, 22.3},
			times:  []int64{baseTime + 2, baseTime + 12, baseTime + 22},
		},
	}

	for _, tc := range testCases {
		for i, val := range tc.values {
			err := db.Write(metric, tc.times[i], val, tc.tags)
			if err != nil {
				t.Fatalf("Failed to write point: %v", err)
			}
		}
	}

	for _, tc := range testCases {
		result, err := db.Read(metric, tc.times[0], tc.times[len(tc.times)-1], tc.tags)
		if err != nil {
			t.Fatalf("Failed to read series: %v", err)
		}

		if len(result.Values) != len(tc.values) {
			t.Fatalf("Expected %d values, got %d", len(tc.values), len(result.Values))
		}

		for i, expectedVal := range tc.values {
			if !almostEqual(result.Values[i], expectedVal) {
				t.Fatalf("Value mismatch at index %d: expected %f, got %f", i, expectedVal, result.Values[i])
			}
			if result.Timestamps[i] != tc.times[i] {
				t.Fatalf("Timestamp mismatch at index %d: expected %d, got %d", i, tc.times[i], result.Timestamps[i])
			}
		}
	}
}

func TestWriteBatch(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024*1024)
	defer cleanup()

	metric := "temperature"
	baseTime := time.Now().Unix()
	tags := map[string]string{"sensor": "temp01", "location": "datacenter"}

	var timestamps []int64
	var values []float64

	for i := range 50 {
		timestamps = append(timestamps, baseTime+int64(i)*60)
		values = append(values, 20.0+rand.Float64()*10.0)
	}

	err := db.WriteBatch(metric, timestamps, values, tags)
	if err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	result, err := db.Read(metric, timestamps[0], timestamps[len(timestamps)-1], tags)
	if err != nil {
		t.Fatalf("Failed to read batch data: %v", err)
	}

	if len(result.Values) != len(values) {
		t.Fatalf("Expected %d values, got %d", len(values), len(result.Values))
	}

	for i, expectedVal := range values {
		if !almostEqual(result.Values[i], expectedVal) {
			t.Fatalf("Batch value mismatch at index %d: expected %f, got %f", i, expectedVal, result.Values[i])
		}
		if result.Timestamps[i] != timestamps[i] {
			t.Fatalf("Batch timestamp mismatch at index %d: expected %d, got %d", i, timestamps[i], result.Timestamps[i])
		}
	}
}

func TestReadWithTimeRange(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024*1024)
	defer cleanup()

	metric := "memory_usage"
	baseTime := time.Now().Unix()
	tags := map[string]string{"process": "app1"}

	allTimestamps := make([]int64, 0, 100)
	allValues := make([]float64, 0, 100)

	for i := range 100 {
		ts := baseTime + int64(i)*60
		val := rand.Float64() * 100

		allTimestamps = append(allTimestamps, ts)
		allValues = append(allValues, val)

		err := db.Write(metric, ts, val, tags)
		if err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}

	startTime := allTimestamps[20]
	endTime := allTimestamps[29]

	result, err := db.Read(metric, startTime, endTime, tags)
	if err != nil {
		t.Fatalf("Failed to read time range: %v", err)
	}

	expectedCount := 10
	if len(result.Values) != expectedCount {
		t.Fatalf("Expected %d values in time range, got %d", expectedCount, len(result.Values))
	}

	for i, ts := range result.Timestamps {
		if ts < startTime || ts > endTime {
			t.Fatalf("Timestamp %d is outside requested range [%d, %d]", ts, startTime, endTime)
		}

		originalIndex := -1
		for j, originalTs := range allTimestamps {
			if originalTs == ts {
				originalIndex = j
				break
			}
		}

		if originalIndex == -1 {
			t.Fatalf("Timestamp %d not found in original data", ts)
		}

		if !almostEqual(result.Values[i], allValues[originalIndex]) {
			t.Fatalf("Value mismatch for timestamp %d: expected %f, got %f",
				ts, allValues[originalIndex], result.Values[i])
		}
	}
}

func TestWriteReadAcrossFlush(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024)
	defer cleanup()

	metric := "network_bytes"
	baseTime := time.Now().Unix()
	tags := map[string]string{"interface": "eth0"}

	var allTimestamps []int64
	var allValues []float64

	for i := range 200 {
		ts := baseTime + int64(i)*30
		val := rand.Float64() * 1000000

		allTimestamps = append(allTimestamps, ts)
		allValues = append(allValues, val)

		err := db.Write(metric, ts, val, tags)
		if err != nil {
			t.Fatalf("Failed to write point %d: %v", i, err)
		}

		if i%50 == 49 {
			err := db.Flush()
			if err != nil {
				t.Fatalf("Failed to flush at point %d: %v", i, err)
			}
		}
	}

	err := db.Flush()
	if err != nil {
		t.Fatalf("Final flush failed: %v", err)
	}

	result, err := db.Read(metric, allTimestamps[0], allTimestamps[len(allTimestamps)-1], tags)
	if err != nil {
		t.Fatalf("Failed to read flushed data: %v", err)
	}

	if len(result.Values) != len(allValues) {
		t.Fatalf("Expected %d values after flush, got %d", len(allValues), len(result.Values))
	}

	for i := 1; i < len(result.Timestamps); i++ {
		if result.Timestamps[i] <= result.Timestamps[i-1] {
			t.Fatalf("Timestamps not sorted: %d <= %d at index %d",
				result.Timestamps[i], result.Timestamps[i-1], i)
		}
	}

	for i, expectedVal := range allValues {
		if !almostEqual(result.Values[i], expectedVal) {
			t.Fatalf("Flushed value mismatch at index %d: expected %f, got %f",
				i, expectedVal, result.Values[i])
		}
		if result.Timestamps[i] != allTimestamps[i] {
			t.Fatalf("Flushed timestamp mismatch at index %d: expected %d, got %d",
				i, allTimestamps[i], result.Timestamps[i])
		}
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "serie-persist-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	metric := "disk_usage"
	baseTime := time.Now().Unix()
	tags := map[string]string{"disk": "/dev/sda1", "mount": "/"}

	var originalTimestamps []int64
	var originalValues []float64

	{
		db, err := NewTSMTree(Config{
			DataDir:       tempDir,
			MaxMemSize:    1024,
			FlushInterval: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to create first database instance: %v", err)
		}

		for i := range 50 {
			ts := baseTime + int64(i)*120
			val := 50.0 + rand.Float64()*40.0

			originalTimestamps = append(originalTimestamps, ts)
			originalValues = append(originalValues, val)

			err = db.Write(metric, ts, val, tags)
			if err != nil {
				t.Fatalf("Failed to write point in first session: %v", err)
			}
		}

		err = db.Flush()
		if err != nil {
			t.Fatalf("Failed to flush in first session: %v", err)
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("Failed to close first database: %v", err)
		}
	}

	{
		db, err := NewTSMTree(Config{
			DataDir:       tempDir,
			MaxMemSize:    1024 * 1024,
			FlushInterval: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to create second database instance: %v", err)
		}
		defer db.Close()

		result, err := db.Read(metric, originalTimestamps[0],
			originalTimestamps[len(originalTimestamps)-1], tags)
		if err != nil {
			t.Fatalf("Failed to read persisted data: %v", err)
		}

		if len(result.Values) != len(originalValues) {
			t.Fatalf("Expected %d persisted values, got %d", len(originalValues), len(result.Values))
		}

		for i, expectedVal := range originalValues {
			if !almostEqual(result.Values[i], expectedVal) {
				t.Fatalf("Persisted value mismatch at index %d: expected %f, got %f",
					i, expectedVal, result.Values[i])
			}
			if result.Timestamps[i] != originalTimestamps[i] {
				t.Fatalf("Persisted timestamp mismatch at index %d: expected %d, got %d",
					i, originalTimestamps[i], result.Timestamps[i])
			}
		}
	}
}

func TestReadNonExistentData(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024*1024)
	defer cleanup()

	result, err := db.Read("non_existent_metric", 0, time.Now().Unix(),
		map[string]string{"tag": "value"})
	if err != nil {
		t.Fatalf("Read should not error for non-existent data: %v", err)
	}

	if len(result.Values) != 0 || len(result.Timestamps) != 0 {
		t.Fatalf("Expected empty result for non-existent data, got %d values", len(result.Values))
	}
}

func TestUnsortedTimestamps(t *testing.T) {
	db, cleanup := createTestTreeInTmpDir(t, 1024*1024)
	defer cleanup()

	metric := "random_metric"
	tags := map[string]string{"source": "test"}
	baseTime := time.Now().Unix()

	unsortedData := []struct {
		timestamp int64
		value     float64
	}{
		{baseTime + 100, 10.0},
		{baseTime + 50, 5.0},
		{baseTime + 200, 20.0},
		{baseTime + 25, 2.5},
		{baseTime + 150, 15.0},
		{baseTime + 75, 7.5},
	}

	for _, data := range unsortedData {
		err := db.Write(metric, data.timestamp, data.value, tags)
		if err != nil {
			t.Fatalf("Failed to write unsorted data: %v", err)
		}
	}

	result, err := db.Read(metric, baseTime, baseTime+300, tags)
	if err != nil {
		t.Fatalf("Failed to read unsorted data: %v", err)
	}

	if len(result.Values) != len(unsortedData) {
		t.Fatalf("Expected %d values, got %d", len(unsortedData), len(result.Values))
	}

	for i := 1; i < len(result.Timestamps); i++ {
		if result.Timestamps[i] <= result.Timestamps[i-1] {
			t.Fatalf("Timestamps not sorted after read: %d <= %d",
				result.Timestamps[i], result.Timestamps[i-1])
		}
	}

	sort.Slice(unsortedData, func(i, j int) bool {
		return unsortedData[i].timestamp < unsortedData[j].timestamp
	})

	for i, sortedData := range unsortedData {
		if !almostEqual(result.Values[i], sortedData.value) {
			t.Fatalf("Sorted value mismatch at index %d: expected %f, got %f",
				i, sortedData.value, result.Values[i])
		}
		if result.Timestamps[i] != sortedData.timestamp {
			t.Fatalf("Sorted timestamp mismatch at index %d: expected %d, got %d",
				i, sortedData.timestamp, result.Timestamps[i])
		}
	}
}
