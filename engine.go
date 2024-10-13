package serie

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Point struct {
	Metric    string
	Tags      map[string]string
	Value     float64
	Timestamp int64
}

type TSMTree struct {
	dataDir     string
	mem         *Memtable
	immutable   []*Memtable
	files       []*TSMFile
	maxMemSize  int
	mu          sync.RWMutex
	flushTicker *time.Ticker
}

type Memtable struct {
	data map[string][]Point
	size int
}

type TSMFile struct {
	path     string
	file     *os.File
	index    map[string][]IndexEntry
	writePos int64
}

type IndexEntry struct {
	MinTime int64
	MaxTime int64
	Offset  int64
	Size    int64
}

func NewTSMTree(dataDir string, maxMemSize int) *TSMTree {
	t := &TSMTree{
		dataDir:    dataDir,
		mem:        &Memtable{data: make(map[string][]Point)},
		maxMemSize: maxMemSize,
	}
	t.flushTicker = time.NewTicker(10 * time.Second)
	return t
}

func (t *TSMTree) Write(key string, timestamp int64, val float64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	p := Point{Timestamp: timestamp, Value: val}
	t.mem.data[key] = append(t.mem.data[key], p)
	t.mem.size += 16

	if t.mem.size >= t.maxMemSize {
		t.immutable = append(t.immutable, t.mem)
		t.mem = &Memtable{data: make(map[string][]Point)}
	}

	return nil
}

func (t *TSMTree) Read(key string, minTime, maxTime int64) ([]Point, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var res []Point
	if points, ok := t.mem.data[key]; ok {
		for _, p := range points {
			if p.Timestamp >= minTime && p.Timestamp <= maxTime {
				res = append(res, p)
			}
		}
	}

	// TODO: Concurrency here
	for _, immutable := range t.immutable {
		if points, ok := immutable.data[key]; ok {
			for _, p := range points {
				if p.Timestamp >= minTime && p.Timestamp <= maxTime {
					res = append(res, p)
				}
			}
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp < res[j].Timestamp
	})

	return res, nil
}

func (t *TSMTree) createTSMFile() (*TSMFile, error) {
	filename := fmt.Sprintf("%d.tsm", time.Now().UnixNano())
	path := filepath.Join(t.dataDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &TSMFile{
		path:     path,
		index:    make(map[string][]IndexEntry),
		writePos: 0,
		file:     file,
	}, nil
}

func (f *TSMFile) writeTimestamps(timestamps []int64) error {
	if len(timestamps) == 0 {
		return nil
	}

	if err := binary.Write(f.file, binary.LittleEndian, timestamps[0]); err != nil {
		return err
	}

	for i := 1; i < len(timestamps); i++ {
		delta := timestamps[i] - timestamps[i-1]
		if err := binary.Write(f.file, binary.LittleEndian, delta); err != nil {
			return err
		}
	}

	f.writePos += int64(len(timestamps) * 8)
	return nil
}

func (f *TSMFile) writeValues(values []float64) error {
	for _, v := range values {
		if err := binary.Write(f.file, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	f.writePos += int64(len(values) * 8)
	return nil
}

func (f *TSMFile) finalize() error {
	indexOffset := f.writePos

	if err := binary.Write(f.file, binary.LittleEndian, uint32(len(f.index))); err != nil {
		return err
	}

	for key, entries := range f.index {
		if err := f.writeKey(key); err != nil {
			return err
		}

		if err := binary.Write(f.file, binary.LittleEndian, uint32(len(entries))); err != nil {
			return err
		}

		for _, entry := range entries {
			if err := binary.Write(f.file, binary.LittleEndian, entry); err != nil {
				return err
			}
		}
	}

	if err := binary.Write(f.file, binary.LittleEndian, indexOffset); err != nil {
		return err
	}

	return f.file.Sync()
}

func (f *TSMFile) write(key string, points []Point) error {
	// Ignore empty points
	if len(points) == 0 {
		return nil
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	offset := f.writePos

	if err := f.writeKey(key); err != nil {
		return err
	}

	if err := binary.Write(f.file, binary.LittleEndian, uint32(len(points))); err != nil {
		return err
	}

	f.writePos += 4 // Update writePos for the uint32 write

	timestamps := make([]int64, len(points))
	for i, p := range points {
		timestamps[i] = p.Timestamp
	}
	if err := f.writeTimestamps(timestamps); err != nil {
		return err
	}

	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}
	if err := f.writeValues(values); err != nil {
		return err
	}

	blockSize := f.writePos - offset

	f.index[key] = append(f.index[key], IndexEntry{
		MinTime: points[0].Timestamp,
		MaxTime: points[len(points)-1].Timestamp,
		Offset:  offset,
		Size:    blockSize,
	})

	return nil
}

func (f *TSMFile) writeKey(key string) error {
	keyLength := uint16(len(key))
	if err := binary.Write(f.file, binary.LittleEndian, keyLength); err != nil {
		return err
	}
	f.writePos += 2 // Update writePos for the uint16 write

	n, err := f.file.WriteString(key)
	if err != nil {
		return err
	}
	f.writePos += int64(n) // Update writePos for the string write

	return nil
}

func (f *TSMFile) read(key string, minTime, maxTime int64) ([]Point, error) {
	entries, ok := f.index[key]
	if !ok {
		return nil, nil
	}

	var results []Point
	for _, entry := range entries {
		if entry.MinTime > maxTime || entry.MaxTime < minTime {
			continue
		}

		if _, err := f.file.Seek(entry.Offset, io.SeekStart); err != nil {
			return nil, err
		}

		var keyLength uint16
		if err := binary.Read(f.file, binary.LittleEndian, &keyLength); err != nil {
			return nil, err
		}
		keyBuffer := make([]byte, keyLength)
		n, err := f.file.Read(keyBuffer)
		if err != nil {
			return nil, err
		}
		if n != int(keyLength) {
			return nil, fmt.Errorf("expected to read %d bytes for key, but read %d", keyLength, n)
		}
		readKey := string(keyBuffer)
		if readKey != key {
			return nil, fmt.Errorf("key mismatch: expected %s, got %s", key, readKey)
		}

		var numPoints uint32
		if err := binary.Read(f.file, binary.LittleEndian, &numPoints); err != nil {
			return nil, err
		}

		timestamps := make([]int64, numPoints)
		if err := binary.Read(f.file, binary.LittleEndian, &timestamps[0]); err != nil {
			return nil, err
		}
		for i := uint32(1); i < numPoints; i++ {
			var delta int64
			if err := binary.Read(f.file, binary.LittleEndian, &delta); err != nil {
				return nil, err
			}
			timestamps[i] = timestamps[i-1] + delta
		}

		values := make([]float64, numPoints)
		if err := binary.Read(f.file, binary.LittleEndian, &values); err != nil {
			return nil, err
		}

		for i, ts := range timestamps {
			if ts >= minTime && ts <= maxTime {
				results = append(results, Point{Timestamp: ts, Value: values[i]})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}
