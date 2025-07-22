package serie

import (
	"sync"
)

type SeriesData struct {
	Timestamps []int64
	Values     []float64
	Tags       TagMap
	Metric     uint32
	capacity   int
	mu         sync.RWMutex
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
	sd.mu.Lock()
	sd.Timestamps = append(sd.Timestamps, timestamp)
	sd.Values = append(sd.Values, value)
	sd.mu.Unlock()
}

func (sd *SeriesData) ReadPoints(mintime, maxtime int64) *ReadResult {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	res := &ReadResult{}
	for i, ts := range sd.Timestamps {
		if ts >= mintime && ts <= maxtime {
			res.Timestamps = append(res.Timestamps, ts)
			res.Values = append(res.Values, sd.Values[i])
		}
	}
	return res
}

type TagMap map[uint32]uint32

func (t TagMap) Hash() uint64 {
	hash := uint64(1)
	for k, v := range t {
		pairHash := uint64(14695981039346656037)
		pairHash ^= uint64(k)
		pairHash *= 1099511628211
		pairHash ^= uint64(v)
		pairHash *= 1099511628211

		if pairHash == 0 {
			pairHash = 1
		}
		hash *= pairHash
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

func (mem *memtable) getSeriesWithKey(key seriesKey) *SeriesData {
	mem.mu.RLock()
	if series, exists := mem.series[key]; exists {
		mem.mu.RUnlock()
		return series
	}
	mem.mu.RUnlock()

	return nil
}

func (mem *memtable) getSeriesOrCreate(metric uint32, tagmap TagMap) *SeriesData {
	key := newSeriesKey(metric, tagmap)

	mem.mu.RLock()
	if series, exists := mem.series[key]; exists {
		mem.mu.RUnlock()
		return series
	}
	mem.mu.RUnlock()

	mem.mu.Lock()
	defer mem.mu.Unlock()

	if series, exists := mem.series[key]; exists {
		return series
	}

	series := NewSeriesData(metric, tagmap, 0)
	mem.series[key] = series
	mem.metricToSeries[metric] = append(mem.metricToSeries[metric], key)
	mem.size += 32 // TODO: fix this

	return series
}

func (mem *memtable) getSeries(metric uint32, tagmap TagMap) *SeriesData {
	key := newSeriesKey(metric, tagmap)
	mem.mu.RLock()
	if series, exists := mem.series[key]; exists {
		mem.mu.RUnlock()
		return series
	}
	mem.mu.RUnlock()

	return nil
}

func (mem *memtable) AddPoint(metric uint32, timestamp int64, value float64, tags TagMap) {
	series := mem.getSeriesOrCreate(metric, tags)
	series.AddPoint(timestamp, value)
}

func (mem *memtable) BatchAddPoints(metric uint32, timestamps []int64, values []float64, tags TagMap) {
	if len(timestamps) != len(values) {
		panic("timestamps and values length mismatch")
	}

	series := mem.getSeriesOrCreate(metric, tags)

	series.mu.Lock()
	defer series.mu.Unlock()

	series.Timestamps = append(series.Timestamps, timestamps...)
	series.Values = append(series.Values, values...)
}
