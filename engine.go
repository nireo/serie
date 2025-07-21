package serie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/rs/zerolog"
)

// engine.go -- handles all of the internal storage of the data. The TSMTree is a LSM-tree that is
// more suitable for time series data as it takes into account timestamps. The entries are encoded
// using snappy before writing. I first looked at gorilla but it doesn't support tags and is more geared
// towards in memory database and I don't want to do that.
//
// Basic overview of the engine:
//   - Newest entries are written into memory. Once a in-memory table exceeds the maximum size
//     it gets stored as a immutable memtable from which it can still be queried from.
//   - A background flush process is executed which takes the immutable tables and writes them
//     into TSM files, which are persistant files on the disk which include a index. That index can
//     easily used to find if a TSM file's contents would fulfill a query.
type Engine interface {
	Write(key string, timestamp int64, val float64) error
	Read(key string, minTime, maxTime int64) ([]Point, error)
	WriteBatch(points []Point) error
	Query(queryStr string) ([]QueryResult, error)
}

// Point represents a single point in a given dataset.
type Point struct {
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

type TSMTree struct {
	log     zerolog.Logger
	dataDir string
	// keeps every tag string and metric name in a reusable place.
	// this makes sense in this context as mostly time series data is
	// redundant
	sp          *StringPool
	mem         *memtable
	immutable   []*memtable
	files       []*TSMFile
	maxMemSize  int
	mu          sync.RWMutex
	flushTicker *time.Ticker
	writeMu     sync.Mutex
	done        chan struct{}
}

// Memtable is the in-memory table to query points fast that more fresh data is easily retrieved.
// The memtables are then pushed into a list of immutable tables which will be flushed to disk.
// The flush timing and the maximum mem size can be configured to fit the system and the workload.
type Memtable struct {
	data map[string][]Point
	size int
}

// TSMFile is a read-only file on disk that contains an index and pointer to path.
// When searching for given metrics and keys in a timeframe we can use the index
// to look at them.
type TSMFile struct {
	mu       sync.RWMutex
	path     string
	file     *os.File
	index    map[uint32][]IndexEntry
	writePos int64
}

// IndexEntry is used that we can efficiently find the points in a given TSM file.
// The offset points to the offset in the file and size determines the size of the
// COMPRESSED size.
type IndexEntry struct {
	MinTime int64
	MaxTime int64
	Offset  int64
	Size    int64
	TagHash int64
}

type Config struct {
	MaxMemSize    int
	DataDir       string
	FlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxMemSize:    1024 * 1024 * 10, // 10 mb
		DataDir:       "./serie",
		FlushInterval: time.Minute * 10,
	}
}

func NewTSMTree(conf Config) (*TSMTree, error) {
	t := &TSMTree{
		dataDir:     conf.DataDir,
		mem:         newMemtable(),
		maxMemSize:  conf.MaxMemSize,
		flushTicker: time.NewTicker(conf.FlushInterval),
		done:        make(chan struct{}),
		mu:          sync.RWMutex{},
		sp:          NewStringPool(),
	}

	err := os.MkdirAll(conf.DataDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	t.log = zerolog.New(os.Stderr).With().Timestamp().Str("component", "engine").Logger()
	t.flushTicker = time.NewTicker(conf.FlushInterval)
	go t.flushBackgroundJob()

	return t, nil
}

func (t *TSMTree) flushBackgroundJob() {
	for {
		select {
		case <-t.done:
			return
		case <-t.flushTicker.C:
			t.log.Info().Msg("starting to flush memtables")
			if err := t.Flush(); err != nil {
				t.log.Err(err).Msg("error flushing during background job")
			}
		}
	}
}

func (t *TSMTree) convertTagsToTagMap(tags map[string]string) TagMap {
	res := make(map[uint32]uint32, len(tags))
	for k, v := range tags {
		res[t.sp.Add(k)] = t.sp.Add(v)
	}

	return res
}

func (t *TSMTree) getMetricIDAndTagMap(metric string, tags map[string]string) (uint32, TagMap) {
	metricID := t.sp.Add(metric)
	tm := t.convertTagsToTagMap(tags)
	return metricID, tm
}

func (t *TSMTree) Write(key string, timestamp int64, val float64, tags map[string]string) error {
	mid, tm := t.getMetricIDAndTagMap(key, tags)
	t.mem.AddPoint(mid, timestamp, val, tm)

	if t.mem.size >= t.maxMemSize {
		// TODO: some kind of lock here
		t.log.Info().Msg("writable memtable reached maximum capacity")
		t.immutable = append(t.immutable, t.mem)
		t.mem = newMemtable()
	}

	return nil
}

func (t *TSMTree) WriteBatch(key string, timestamps []int64, vals []float64, seriesTags map[string]string) error {
	mid, tm := t.getMetricIDAndTagMap(key, seriesTags)
	t.mem.BatchAddPoints(mid, timestamps, vals, tm)

	// TODO: implement some kind of checking for the points such that it doesnt go way past the limit for the memtable.
	if t.mem.size >= t.maxMemSize {
		// TODO: some kind of lock here
		t.log.Info().Msg("writable memtable reached maximum capacity")
		t.immutable = append(t.immutable, t.mem)
		t.mem = newMemtable()
	}

	return nil
}

type ReadResult struct {
	Values     []float64
	Timestamps []int64
}

func (t *TSMTree) Read(key string, minTime, maxTime int64, tags map[string]string) (*ReadResult, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	mid, tm := t.getMetricIDAndTagMap(key, tags)
	series := t.mem.getSeries(mid, tm)

	return series.ReadPoints(minTime, maxTime), nil
}

func (t *TSMTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.immutable) == 0 {
		return nil
	}

	t.log.Info().Int("tableCount", len(t.immutable)).Msg("flushing tableCount amount of tables")
	var wg sync.WaitGroup
	errorChan := make(chan error, len(t.immutable))

	for _, table := range t.immutable {
		wg.Add(1)
		go func(mt *memtable) {
			defer wg.Done()
			if err := t.flushMemtable(mt); err != nil {
				errorChan <- err
			}
		}(table)
	}

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	t.immutable = nil // reset the array
	return nil
}

func (t *TSMTree) Close() error {
	t.log.Info().Msg("closing the database")

	if err := t.Flush(); err != nil {
		return err
	}

	for _, f := range t.files {
		if err := f.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (t *TSMTree) flushMemtable(table *memtable) error {
	t.log.Info().Msg("creating a tsm file from memtable")
	file, err := t.createTSMFile()
	if err != nil {
		return err
	}

	for _, series := range table.series {
		if err := file.writeSeriesBlock(series); err != nil {
			return err
		}
	}

	t.log.Info().Msg("wrote points into tsm file")
	if err := file.createIndexFile(); err != nil {
		return err
	}

	t.log.Info().Msg("created a tsm index file")
	if err := file.file.Sync(); err != nil {
		return err
	}
	t.log.Info().Msg("finalized tsm index file")

	t.files = append(t.files, file)
	return nil
}

func (f *TSMFile) writeSeriesBlock(series *SeriesData) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(series.Timestamps) == 0 {
		return nil
	}

	indices := make([]int, len(series.Timestamps))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return series.Timestamps[indices[i]] < series.Timestamps[indices[j]]
	})

	sortedTimestamps := make([]int64, len(series.Timestamps))
	sortedValues := make([]float64, len(series.Values))
	for i, idx := range indices {
		sortedTimestamps[i] = series.Timestamps[idx]
		sortedValues[i] = series.Values[idx]
	}

	offset := f.writePos

	var payload bytes.Buffer
	deltaTimestamps := deltaEncode(sortedTimestamps)

	for _, ts := range deltaTimestamps {
		binary.Write(&payload, binary.LittleEndian, ts)
	}

	for _, val := range sortedValues {
		binary.Write(&payload, binary.LittleEndian, val)
	}

	compressed := snappy.Encode(nil, payload.Bytes())

	var header bytes.Buffer
	blockSize := uint32(4 + 4 + 2 + 4 + 8 + 8 + 1 + len(series.Tags)*8 + 4 + len(compressed))
	binary.Write(&header, binary.LittleEndian, blockSize)
	binary.Write(&header, binary.LittleEndian, series.Metric)
	binary.Write(&header, binary.LittleEndian, uint16(len(series.Tags)))
	binary.Write(&header, binary.LittleEndian, uint32(len(series.Timestamps)))
	binary.Write(&header, binary.LittleEndian, sortedTimestamps[0])
	binary.Write(&header, binary.LittleEndian, sortedTimestamps[len(sortedTimestamps)-1])
	binary.Write(&header, binary.LittleEndian, uint8(1))

	for tagKey, tagVal := range series.Tags {
		binary.Write(&header, binary.LittleEndian, tagKey)
		binary.Write(&header, binary.LittleEndian, tagVal)
	}

	binary.Write(&header, binary.LittleEndian, uint32(len(compressed)))

	if _, err := f.file.Write(header.Bytes()); err != nil {
		return err
	}
	if _, err := f.file.Write(compressed); err != nil {
		return err
	}

	f.writePos += int64(blockSize)

	tagHash := series.Tags.Hash()
	f.index[series.Metric] = append(f.index[series.Metric], IndexEntry{
		TagHash: int64(tagHash),
		MinTime: sortedTimestamps[0],
		MaxTime: sortedTimestamps[len(sortedTimestamps)-1],
		Offset:  offset,
		Size:    int64(blockSize),
	})

	return nil
}

func (f *TSMFile) readSeriesBlock(metricID uint32, tagHash uint64, minTime, maxTime int64) (*ReadResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	entries, exists := f.index[metricID]
	if !exists {
		return &ReadResult{}, nil
	}

	for _, entry := range entries {
		if entry.TagHash != int64(tagHash) {
			continue
		}
		if entry.MinTime > maxTime || entry.MaxTime < minTime {
			continue
		}

		headerSize := int64(4 + 4 + 2 + 4 + 8 + 8 + 1)
		headerData := make([]byte, headerSize)
		if _, err := f.file.ReadAt(headerData, entry.Offset); err != nil {
			return nil, err
		}

		buf := bytes.NewReader(headerData)
		var blockSize uint32
		var metric uint32
		var tagCount uint16
		var pointCount uint32
		var minTimestamp, maxTimestamp int64
		var compressionType uint8

		binary.Read(buf, binary.LittleEndian, &blockSize)
		binary.Read(buf, binary.LittleEndian, &metric)
		binary.Read(buf, binary.LittleEndian, &tagCount)
		binary.Read(buf, binary.LittleEndian, &pointCount)
		binary.Read(buf, binary.LittleEndian, &minTimestamp)
		binary.Read(buf, binary.LittleEndian, &maxTimestamp)
		binary.Read(buf, binary.LittleEndian, &compressionType)

		tagPairsSize := int64(tagCount) * 8
		dataOffset := entry.Offset + headerSize + tagPairsSize

		var compressedSize uint32
		compSizeData := make([]byte, 4)
		if _, err := f.file.ReadAt(compSizeData, dataOffset); err != nil {
			return nil, err
		}
		binary.Read(bytes.NewReader(compSizeData), binary.LittleEndian, &compressedSize)

		compressedData := make([]byte, compressedSize)
		if _, err := f.file.ReadAt(compressedData, dataOffset+4); err != nil {
			return nil, err
		}

		decompressed, err := snappy.Decode(nil, compressedData)
		if err != nil {
			return nil, err
		}

		dataBuf := bytes.NewReader(decompressed)

		deltaTimestamps := make([]int64, pointCount)
		for i := uint32(0); i < pointCount; i++ {
			binary.Read(dataBuf, binary.LittleEndian, &deltaTimestamps[i])
		}

		values := make([]float64, pointCount)
		for i := uint32(0); i < pointCount; i++ {
			binary.Read(dataBuf, binary.LittleEndian, &values[i])
		}

		timestamps := deltaDecode(deltaTimestamps)

		result := &ReadResult{}
		for i, ts := range timestamps {
			if ts >= minTime && ts <= maxTime {
				result.Timestamps = append(result.Timestamps, ts)
				result.Values = append(result.Values, values[i])
			}
		}

		return result, nil
	}

	return &ReadResult{}, nil
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
		index:    make(map[uint32][]IndexEntry),
		writePos: 0,
		file:     file,
	}, nil
}

func (f *TSMFile) write(key string, points []Point) error {
	// if len(points) == 0 {
	// 	return nil
	// }
	//
	// f.mu.Lock()
	// defer f.mu.Unlock()
	//
	// sort.Slice(points, func(i, j int) bool {
	// 	return points[i].Timestamp < points[j].Timestamp
	// })
	//
	// offset := f.writePos
	//
	// // Prepare data for compression
	// var buf bytes.Buffer
	// if err := binary.Write(&buf, binary.LittleEndian, uint32(len(points))); err != nil {
	// 	return err
	// }
	//
	// for _, p := range points {
	// 	// write basic stuff
	// 	if err := binary.Write(&buf, binary.LittleEndian, p.Timestamp); err != nil {
	// 		return err
	// 	}
	//
	// 	if err := binary.Write(&buf, binary.LittleEndian, p.Value); err != nil {
	// 		return err
	// 	}
	//
	// 	if err := binary.Write(&buf, binary.LittleEndian, uint8(len(p.Tags))); err != nil {
	// 		return err
	// 	}
	//
	// 	for tag, val := range p.Tags {
	// 		if err := binary.Write(&buf, binary.LittleEndian, uint8(len(tag))); err != nil {
	// 			return err
	// 		}
	//
	// 		if _, err := buf.WriteString(tag); err != nil {
	// 			return err
	// 		}
	//
	// 		if err := binary.Write(&buf, binary.LittleEndian, uint8(len(val))); err != nil {
	// 			return err
	// 		}
	//
	// 		if _, err := buf.WriteString(val); err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	//
	// // Compress and write data
	// compressedData := snappy.Encode(nil, buf.Bytes())
	//
	// // Write the size of the compressed data first
	// if err := binary.Write(f.file, binary.LittleEndian, uint32(len(compressedData))); err != nil {
	// 	return err
	// }
	//
	// f.writePos += 4
	//
	// n, err := f.file.Write(compressedData)
	// if err != nil {
	// 	return err
	// }
	// f.writePos += int64(n)
	//
	// blockSize := f.writePos - offset
	// f.index[key] = append(f.index[key], IndexEntry{
	// 	MinTime: points[0].Timestamp,
	// 	MaxTime: points[len(points)-1].Timestamp,
	// 	Offset:  offset,
	// 	Size:    blockSize,
	// })
	//
	// return nil
	return nil
}

func deltaEncode(timestamps []int64) []int64 {
	if len(timestamps) == 0 {
		return nil
	}
	deltas := make([]int64, len(timestamps))
	deltas[0] = timestamps[0]
	for i := 1; i < len(timestamps); i++ {
		deltas[i] = timestamps[i] - timestamps[i-1]
	}
	return deltas
}

func deltaDecode(deltas []int64) []int64 {
	if len(deltas) == 0 {
		return nil
	}

	timestamps := make([]int64, len(deltas))
	timestamps[0] = deltas[0]
	for i := 1; i < len(deltas); i++ {
		timestamps[i] = timestamps[i-1] + deltas[i]
	}
	return timestamps
}

func (t *TSMFile) encodeIndex() ([]byte, error) {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.LittleEndian, uint16(len(t.index))); err != nil {
		return nil, err
	}

	for metric, entries := range t.index {
		if err := binary.Write(&b, binary.LittleEndian, metric); err != nil {
			return nil, err
		}

		if err := binary.Write(&b, binary.LittleEndian, uint16(len(entries))); err != nil {
			return nil, err
		}

		for _, entry := range entries {
			if err := binary.Write(&b, binary.LittleEndian, entry.MinTime); err != nil {
				return nil, err
			}

			if err := binary.Write(&b, binary.LittleEndian, entry.MaxTime); err != nil {
				return nil, err
			}

			if err := binary.Write(&b, binary.LittleEndian, entry.Offset); err != nil {
				return nil, err
			}

			if err := binary.Write(&b, binary.LittleEndian, entry.Size); err != nil {
				return nil, err
			}

			if err := binary.Write(&b, binary.LittleEndian, entry.TagHash); err != nil {
				return nil, err
			}
		}
	}

	return snappy.Encode(nil, b.Bytes()), nil
}

func (t *TSMFile) decodeIndex(data []byte) error {
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	buf := bytes.NewReader(decompressed)

	// read size so we can preallocate the index map
	var indexSize uint16
	if err := binary.Read(buf, binary.LittleEndian, &indexSize); err != nil {
		return err
	}

	if t.index == nil {
		t.index = make(map[uint32][]IndexEntry, indexSize)
	}

	for i := uint16(0); i < indexSize; i++ {
		var metric uint32
		if err := binary.Read(buf, binary.LittleEndian, &metric); err != nil {
			return err
		}

		var entriesSize uint16
		if err := binary.Read(buf, binary.LittleEndian, &entriesSize); err != nil {
			return err
		}

		indexes := make([]IndexEntry, 0, entriesSize)
		for j := uint16(0); j < entriesSize; j++ {
			indexEntry := IndexEntry{}
			if err := binary.Read(buf, binary.LittleEndian, &indexEntry.MinTime); err != nil {
				return err
			}

			if err := binary.Read(buf, binary.LittleEndian, &indexEntry.MaxTime); err != nil {
				return err
			}

			if err := binary.Read(buf, binary.LittleEndian, &indexEntry.Offset); err != nil {
				return err
			}

			if err := binary.Read(buf, binary.LittleEndian, &indexEntry.Size); err != nil {
				return err
			}

			if err := binary.Read(buf, binary.LittleEndian, &indexEntry.TagHash); err != nil {
				return err
			}

			indexes = append(indexes, indexEntry)
		}
		t.index[metric] = indexes
	}

	return nil
}

func (t *TSMFile) createIndexFile() error {
	indexPath := t.path[0:len(t.path)-4] + ".idx"
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer indexFile.Close() // we don't need to keep the indexFile open since it won't be written to

	encodedIndex, err := t.encodeIndex()
	if err != nil {
		return err
	}

	// write the snappy compressed data into the file
	if _, err := indexFile.Write(encodedIndex); err != nil {
		return err
	}

	return nil
}

// parseDataDir takes all of the index files in the data dir and then
// builds in the TSM files from the directory and reads the indicies
func (t *TSMTree) parseDataDir() error {
	entries, err := os.ReadDir(t.dataDir)
	if err != nil {
		return err
	}

	var tsmFiles []*TSMFile
	var wg sync.WaitGroup
	errChan := make(chan error, len(entries))
	tsmFileChan := make(chan *TSMFile)

	for _, entry := range entries {
		if entry.IsDir() { // dirs shouldn't exist but just make sure
			continue
		}

		// ignore every file that is not an index.
		if !strings.HasSuffix(entry.Name(), ".idx") {
			continue
		}

		wg.Add(1)
		go func(entry os.DirEntry) {
			defer wg.Done()
			entryName := entry.Name()

			tsmPath := path.Join(t.dataDir, entryName[:len(entryName)-4]+".tsm")
			tsmFile := &TSMFile{
				path: tsmPath,
			}
			fileData, err := os.ReadFile(path.Join(t.dataDir, entryName))
			if err != nil {
				errChan <- fmt.Errorf("error reading file %s: %w", entryName, err)
				return
			}

			if err = tsmFile.decodeIndex(fileData); err != nil {
				errChan <- fmt.Errorf("error decoding index for file %s: %w", entryName, err)
				return
			}

			// open the actual tsm file such that we can read from it
			file, err := os.Open(tsmFile.path)
			if err != nil {
				errChan <- fmt.Errorf("error decoding index for file %s: %w", entryName, err)
				return
			}

			tsmFile.file = file
			tsmFileChan <- tsmFile
		}(entry)
	}

	go func() {
		wg.Wait()
		close(errChan)
		close(tsmFileChan)
	}()

	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
			} else if err != nil {
				return err
			}
		case tsmFile, ok := <-tsmFileChan:
			if !ok {
				tsmFileChan = nil
			} else {
				tsmFiles = append(tsmFiles, tsmFile)
			}
		}
		if errChan == nil && tsmFileChan == nil {
			break
		}
	}

	t.files = tsmFiles
	return nil
}

const (
	aggFloat = iota
	aggInt
)

type aggregateResult struct {
	intValue int64
	floatVal float64
	kind     int
}

func sumUpValues(points []float64) float64 {
	pointSum := float64(0.0)
	for _, p := range points {
		pointSum += p
	}

	return pointSum
}

func (t *TSMTree) aggregate(funcName, metric string, minTime, maxTime int64, tags map[string]string) (aggregateResult, error) {
	res, err := t.Read(metric, minTime, maxTime, tags)
	if err != nil {
		return aggregateResult{}, err
	}

	if len(res.Timestamps) == 0 {
		return aggregateResult{}, errors.New("no points to aggregate")
	}

	switch funcName {
	case "avg":
		return aggregateResult{floatVal: sumUpValues(res.Values) / float64(len(res.Values)), kind: aggFloat}, nil
	case "sum":
		return aggregateResult{floatVal: sumUpValues(res.Values), kind: aggFloat}, nil
	case "count":
		return aggregateResult{intValue: int64(len(res.Values)), kind: aggInt}, nil
	case "min":
		return aggregateResult{floatVal: res.Values[0], kind: aggFloat}, nil // already sorted so the smallest element is the first element
	case "max":
		return aggregateResult{floatVal: res.Values[len(res.Values)-1], kind: aggFloat}, nil // already sorted so the smallest element is the first element
	}

	return aggregateResult{}, errors.New("unrecognized function")
}

type QueryResult struct {
	Aggregate string
	Result    map[string]float64
}

func aggregateMultipleTags(aggregate string, points []Point, pointTags []string) (QueryResult, error) {
	if len(points) == 0 {
		return QueryResult{}, fmt.Errorf("no points to aggregate")
	}
	if len(points) != len(pointTags) {
		return QueryResult{}, fmt.Errorf("points and pointTags length mismatch")
	}

	switch aggregate {
	case "sum":
		return sumMultipleTags(points, pointTags)
	case "avg":
		return avgMultipleTags(points, pointTags)
	case "min":
		return minMultipleTags(points, pointTags)
	case "max":
		return maxMultipleTags(points, pointTags)
	case "count":
		return countMultipleTags(points, pointTags)
	case "stddev":
		return stddevMultipleTags(points, pointTags)
	case "median":
		return medianMultipleTags(points, pointTags)
	case "percentile90":
		return percentile90MultipleTags(points, pointTags)
	case "range":
		return rangeMultipleTags(points, pointTags)
	default:
		return QueryResult{}, fmt.Errorf("unsupported aggregation function: %s", aggregate)
	}
}

func (t *TSMTree) Query(queryStr string) ([]QueryResult, error) {
	// query, err := parseQuery(queryStr)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// // It is expected that the resulting table has the same tags such that every point queried
	// // has the same tags.
	//
	// groupBys := make(chan QueryResult)
	//
	// points, err := t.Read(query.metric, query.timeStart, query.timeEnd)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// // pointsTags[i] is the wanted tags of point[i] in a string form such that they're easy to perform calculations on.
	// var sb strings.Builder
	// pointTags := make([]string, 0, len(points))
	// for _, p := range points {
	// 	for i, wantedTag := range query.groupBy {
	// 		_, err := sb.WriteString(p.Tags[wantedTag])
	// 		if err != nil {
	// 			return nil, err
	// 		}
	//
	// 		if i < len(query.groupBy)-1 {
	// 			if err := sb.WriteByte(','); err != nil {
	// 				return nil, err
	// 			}
	// 		}
	// 	}
	//
	// 	pointTags = append(pointTags, sb.String())
	// 	sb.Reset()
	// }
	//
	// var wg sync.WaitGroup
	// for _, aggregate := range query.aggregates {
	// 	wg.Add(1)
	// 	go func(aggregateFunc string) {
	// 		defer wg.Done()
	//
	// 		queryRes, err := aggregateMultipleTags(aggregateFunc, points, pointTags)
	// 		if err != nil {
	// 			t.log.Err(err).Msg("failed to aggregate query")
	// 			return
	// 		}
	//
	// 		groupBys <- queryRes
	// 	}(aggregate)
	// }
	//
	// go func() {
	// 	wg.Wait()
	// 	close(groupBys)
	// }()
	//
	// var res []QueryResult
	// for m := range groupBys {
	// 	res = append(res, m)
	// }

	return nil, nil
}
