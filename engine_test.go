package serie

import (
	"math"
	"math/rand/v2"
	"os"
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
