package serie

import "sync"

type SeriesData struct {
	Timestamps []int64
	Values     []float64
	Tags       TagMap
	Metric     uint32
	capacity   int
}

func NewSeriesData(metric uint32, tags TagMap, initialCapacity int) *SeriesData {
	if initialCapacity == 0 {
		initialCapacity = 64
	}

	return &SeriesData{
		Timestamps: make([]int64, 0, initialCapacity),
		Values:     make([]float64, 0, initialCapacity),
		Tags:       tags,
		Metric:     metric,
		capacity:   initialCapacity,
	}
}

func (sd *SeriesData) AddPoint(timestamp int64, value float64) {
	sd.Timestamps = append(sd.Timestamps, timestamp)
	sd.Values = append(sd.Values, value)
}

type TagMap map[uint32]uint32

func (t TagMap) Hash() uint64 {
	var hash uint64 = 14695981039346656037 // FNV offset basis
	const prime uint64 = 1099511628211     // FNV prime

	for k, v := range t {
		hash ^= uint64(k)
		hash *= prime
		hash ^= uint64(v)
		hash *= prime
	}
	return hash
}

type seriesKey struct {
	metric  uint32
	tagHash uint64
}

func newSeriesKey(metric uint32, tags TagMap) seriesKey {
	return seriesKey{
		metric:  metric,
		tagHash: tags.Hash(),
	}
}

type memtable struct {
	series         map[seriesKey]*SeriesData
	metricToSeries map[uint32][]seriesKey
	size           int
	mu             sync.RWMutex
}

func newMemtable() *memtable {
	return &memtable{
		series:         make(map[seriesKey]*SeriesData),
		metricToSeries: make(map[uint32][]seriesKey),
		size:           0,
	}
}

func (mem *memtable) AddPoint(metric uint32, timestamp int64, value float64, tags TagMap) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	seriesKey := newSeriesKey(metric, tags)
	series, exists := mem.series[seriesKey]
	if !exists {
		series = NewSeriesData(metric, tags, 0)
		mem.series[seriesKey] = series
		mem.metricToSeries[metric] = append(mem.metricToSeries[metric], seriesKey)
		mem.size += 32 // TODO: fix this
	}

	series.AddPoint(timestamp, value)
}
