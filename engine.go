package serie

import (
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
	path  string
	index map[string][]IndexEntry
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
