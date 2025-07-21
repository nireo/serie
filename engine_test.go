package serie

import (
	"math"
	"math/rand/v2"
	"os"
	"reflect"
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
		index:    make(map[uint32][]IndexEntry),
		writePos: 0,
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
