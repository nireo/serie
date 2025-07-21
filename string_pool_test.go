package serie

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStringPool(t *testing.T) {
	n := 10000
	alphabet := "abcdefgijklmnopqrstuwyzABCDEFGIJKLMNOPRQRSTUWYZ"
	strs := make([]string, n)
	ids := make([]uint32, n)
	sp := NewStringPool()

	for i := range n {
		var sb strings.Builder
		for range 20 {
			sb.WriteByte(alphabet[int(rand.Int31n(int32(len(alphabet))))])
		}
		id := sp.Add(sb.String())
		strs[i] = sb.String()
		ids[i] = id
	}

	for i, s := range strs {
		strid := ids[i]
		newID := sp.Add(s)
		if strid != newID {
			t.Fatalf("new id generated for string %s old id=%d new id=%d", s, strid, newID)
		}
	}

	for i, id := range ids {
		s := sp.Get(id)
		if s == "" {
			t.Fatal("string not found")
		}
		if strs[i] != s {
			t.Fatalf("mismatch strings %s | %s", s, strs[i])
		}
	}
}

func TestStringPoolEmpty(t *testing.T) {
	sp := NewStringPool()

	id := sp.Add("")
	if id == 0 {
		t.Fatal("empty string should get a valid ID")
	}

	retrieved := sp.Get(id)
	if retrieved != "" {
		t.Fatalf("expected empty string, got %q", retrieved)
	}
}

func TestStringPoolInvalidGet(t *testing.T) {
	sp := NewStringPool()

	result := sp.Get(0)
	if result != "" {
		t.Fatalf("expected empty string for ID 0, got %q", result)
	}

	result = sp.Get(999)
	if result != "" {
		t.Fatalf("expected empty string for non-existent ID, got %q", result)
	}
}

func TestStringPoolDuplicates(t *testing.T) {
	sp := NewStringPool()

	testString := "duplicate test"

	id1 := sp.Add(testString)
	id2 := sp.Add(testString)
	id3 := sp.Add(testString)

	if id1 != id2 || id2 != id3 {
		t.Fatalf("duplicate strings should have same ID: %d, %d, %d", id1, id2, id3)
	}

	retrieved := sp.Get(id1)
	if retrieved != testString {
		t.Fatalf("expected %q, got %q", testString, retrieved)
	}
}

func TestStringPoolConcurrency(t *testing.T) {
	sp := NewStringPool()
	numGoroutines := 10
	numStringsPerGoroutine := 1000

	var wg sync.WaitGroup
	results := make([][]uint32, numGoroutines)
	testStrings := make([][]string, numGoroutines)

	for i := range numGoroutines {
		testStrings[i] = make([]string, numStringsPerGoroutine)
		for j := range numStringsPerGoroutine {
			testStrings[i][j] = generateRandomString(20)
		}
	}

	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			results[goroutineID] = make([]uint32, numStringsPerGoroutine)
			for j, s := range testStrings[goroutineID] {
				results[goroutineID][j] = sp.Add(s)
			}
		}(i)
	}

	wg.Wait()

	for i := range numGoroutines {
		for j := range numStringsPerGoroutine {
			retrieved := sp.Get(results[i][j])
			if retrieved != testStrings[i][j] {
				t.Fatalf("concurrent test failed: expected %q, got %q", testStrings[i][j], retrieved)
			}
		}
	}
}

func TestPersistentStringPoolBasic(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "test.pool")

	psp, err := NewPersistantStringPool(filepath)
	if err != nil {
		t.Fatal(err)
	}
	defer psp.Close()

	testStrings := []string{"hello", "world", "test", "string", "pool"}
	ids := make([]uint32, len(testStrings))

	for i, s := range testStrings {
		id, err := psp.Add(s)
		if err != nil {
			t.Fatalf("failed to add string %q: %v", s, err)
		}
		ids[i] = id
	}

	for i, expectedString := range testStrings {
		retrieved := psp.sp.Get(ids[i])
		if retrieved != expectedString {
			t.Fatalf("expected %q, got %q", expectedString, retrieved)
		}
	}

	fileInfo, err := os.Stat(filepath)
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Size() == 0 {
		t.Fatal("file should not be empty")
	}
}

func TestPersistentStringPoolDuplicates(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "test_dup.pool")

	psp, err := NewPersistantStringPool(filepath)
	if err != nil {
		t.Fatal(err)
	}
	defer psp.Close()

	testString := "duplicate test"

	id1, err1 := psp.Add(testString)
	id2, err2 := psp.Add(testString)
	id3, err3 := psp.Add(testString)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatal("unexpected errors adding duplicate strings")
	}

	if id1 != id2 || id2 != id3 {
		t.Fatalf("duplicate strings should have same ID: %d, %d, %d", id1, id2, id3)
	}

	initialSize := getFileSize(t, filepath)

	id4, err := psp.Add(testString)
	if err != nil {
		t.Fatal("unexpected error adding duplicate string again")
	}
	if id4 != id1 {
		t.Fatal("duplicate string should return same ID")
	}

	finalSize := getFileSize(t, filepath)
	if finalSize != initialSize {
		t.Fatal("file should not grow when adding duplicate strings")
	}
}

func TestPersistentStringPoolFileFormat(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)
	filepath := filepath.Join(tempDir, "test_format.pool")

	psp, err := NewPersistantStringPool(filepath)
	if err != nil {
		t.Fatal(err)
	}

	testString := "format test"
	expectedID := uint32(1)

	id, err := psp.Add(testString)
	if err != nil {
		t.Fatal(err)
	}
	if id != expectedID {
		t.Fatalf("expected ID %d, got %d", expectedID, id)
	}

	psp.Close()

	data, err := os.ReadFile(filepath)
	if err != nil {
		t.Fatal(err)
	}

	expectedSize := 2 + len(testString) + 4
	if len(data) != expectedSize {
		t.Fatalf("expected file size %d, got %d", expectedSize, len(data))
	}

	length := binary.LittleEndian.Uint16(data[0:2])
	if length != uint16(len(testString)) {
		t.Fatalf("expected length %d, got %d", len(testString), length)
	}

	stringData := string(data[2 : 2+len(testString)])
	if stringData != testString {
		t.Fatalf("expected string %q, got %q", testString, stringData)
	}

	idData := binary.LittleEndian.Uint32(data[2+len(testString):])
	if idData != expectedID {
		t.Fatalf("expected ID %d, got %d", expectedID, idData)
	}
}

func TestPersistentStringPoolConcurrency(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "test_concurrent.pool")

	psp, err := NewPersistantStringPool(filepath)
	if err != nil {
		t.Fatal(err)
	}
	defer psp.Close()

	numGoroutines := 10
	numStringsPerGoroutine := 100

	var wg sync.WaitGroup
	results := make([][]uint32, numGoroutines)
	testStrings := make([][]string, numGoroutines)
	errors := make([][]error, numGoroutines)

	for i := range numGoroutines {
		testStrings[i] = make([]string, numStringsPerGoroutine)
		for j := range numStringsPerGoroutine {
			testStrings[i][j] = generateRandomString(20) + "_" + string(rune(i)) + "_" + string(rune(j))
		}
	}

	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			results[goroutineID] = make([]uint32, numStringsPerGoroutine)
			errors[goroutineID] = make([]error, numStringsPerGoroutine)

			for j, s := range testStrings[goroutineID] {
				results[goroutineID][j], errors[goroutineID][j] = psp.Add(s)
			}
		}(i)
	}

	wg.Wait()

	for i := range numGoroutines {
		for j := range numStringsPerGoroutine {
			if errors[i][j] != nil {
				t.Fatalf("error in goroutine %d, string %d: %v", i, j, errors[i][j])
			}
		}
	}

	for i := range numGoroutines {
		for j := range numStringsPerGoroutine {
			retrieved := psp.sp.Get(results[i][j])
			if retrieved != testStrings[i][j] {
				t.Fatalf("concurrent test failed: expected %q, got %q", testStrings[i][j], retrieved)
			}
		}
	}
}

func TestLoadPersistantStringPool(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "test_load.pool")

	originalStrings := []string{"hello", "world", "test", "load", "function"}
	var originalIDs []uint32

	{
		psp, err := NewPersistantStringPool(filepath)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range originalStrings {
			id, err := psp.Add(s)
			if err != nil {
				t.Fatalf("failed to add string %q: %v", s, err)
			}
			originalIDs = append(originalIDs, id)
		}

		psp.Close()
	}

	loadedPsp, err := LoadPersistantStringPool(filepath)
	if err != nil {
		t.Fatalf("failed to load persistent string pool: %v", err)
	}
	defer loadedPsp.Close()

	for i, expectedString := range originalStrings {
		expectedID := originalIDs[i]

		retrieved := loadedPsp.sp.Get(expectedID)
		if retrieved != expectedString {
			t.Fatalf("ID %d: expected %q, got %q", expectedID, expectedString, retrieved)
		}

		actualID, err := loadedPsp.Add(expectedString)
		if err != nil {
			t.Fatalf("error re-adding string %q: %v", expectedString, err)
		}
		if actualID != expectedID {
			t.Fatalf("string %q: expected ID %d, got %d", expectedString, expectedID, actualID)
		}
	}

	newString := "newly added"
	newID, err := loadedPsp.Add(newString)
	if err != nil {
		t.Fatalf("failed to add new string: %v", err)
	}

	retrievedNew := loadedPsp.sp.Get(newID)
	if retrievedNew != newString {
		t.Fatalf("new string: expected %q, got %q", newString, retrievedNew)
	}

	maxOriginalID := uint32(0)
	for _, id := range originalIDs {
		if id > maxOriginalID {
			maxOriginalID = id
		}
	}
	if newID <= maxOriginalID {
		t.Fatalf("new ID %d should be greater than max original ID %d", newID, maxOriginalID)
	}
}

func TestLoadPersistantStringPoolLargeFile(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "large.pool")

	numStrings := 1000
	testStrings := make([]string, numStrings)
	var originalIDs []uint32

	for i := range numStrings {
		testStrings[i] = fmt.Sprintf("test_string_%d_%s", i, generateRandomString(20))
	}

	{
		psp, err := NewPersistantStringPool(filepath)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range testStrings {
			id, err := psp.Add(s)
			if err != nil {
				t.Fatalf("failed to add string %q: %v", s, err)
			}
			originalIDs = append(originalIDs, id)
		}

		psp.Close()
	}

	loadedPsp, err := LoadPersistantStringPool(filepath)
	if err != nil {
		t.Fatalf("failed to load large file: %v", err)
	}
	defer loadedPsp.Close()

	for i, expectedString := range testStrings {
		expectedID := originalIDs[i]
		retrieved := loadedPsp.sp.Get(expectedID)
		if retrieved != expectedString {
			t.Fatalf("large file test failed at index %d: expected %q, got %q", i, expectedString, retrieved)
		}
	}
}

func TestLoadPersistantStringFull(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	filepath := filepath.Join(tempDir, "continue.pool")

	phase1Strings := []string{"phase1_a", "phase1_b", "phase1_c"}
	var phase1IDs []uint32

	{
		psp, err := NewPersistantStringPool(filepath)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range phase1Strings {
			id, err := psp.Add(s)
			if err != nil {
				t.Fatal(err)
			}
			phase1IDs = append(phase1IDs, id)
		}

		psp.Close()
	}

	phase2Strings := []string{"phase2_x", "phase2_y", "phase2_z"}
	var phase2IDs []uint32

	{
		psp, err := LoadPersistantStringPool(filepath)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range phase2Strings {
			id, err := psp.Add(s)
			if err != nil {
				t.Fatal(err)
			}
			phase2IDs = append(phase2IDs, id)
		}

		psp.Close()
	}

	{
		psp, err := LoadPersistantStringPool(filepath)
		if err != nil {
			t.Fatal(err)
		}
		defer psp.Close()

		for i, s := range phase1Strings {
			retrieved := psp.sp.Get(phase1IDs[i])
			if retrieved != s {
				t.Fatalf("phase 1 string %d: expected %q, got %q", i, s, retrieved)
			}
		}

		for i, s := range phase2Strings {
			retrieved := psp.sp.Get(phase2IDs[i])
			if retrieved != s {
				t.Fatalf("phase 2 string %d: expected %q, got %q", i, s, retrieved)
			}
		}

		allIDs := append(phase1IDs, phase2IDs...)
		idSet := make(map[uint32]bool)
		for _, id := range allIDs {
			if idSet[id] {
				t.Fatalf("duplicate ID found: %d", id)
			}
			idSet[id] = true
		}
	}
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	var sb strings.Builder
	sb.Grow(length)
	for range length {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	return sb.String()
}

func getFileSize(t *testing.T, filepath string) int64 {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		t.Fatal(err)
	}
	return fileInfo.Size()
}
