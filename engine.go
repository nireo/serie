package serie

import (
	"sync"
	"time"
)

type Point struct {
	Metric    string
	Tags      map[string]string
	Value     float64
	Timestamp time.Time
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
	// TODO: background flush
	return t
}
