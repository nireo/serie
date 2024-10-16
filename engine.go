package serie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
// - Newest entries are written into memory. Once a in-memory table exceeds the maximum size
//   it gets stored as a immutable memtable from which it can still be queried from.
// - A background flush process is executed which takes the immutable tables and writes them
//   into TSM files, which are persistant files on the disk which include a index. That index can
//   easily used to find if a TSM file's contents would fulfill a query.

type Engine interface {
	Write(key string, timestamp int64, val float64) error
	Read(key string, minTime, maxTime int64) ([]Point, error)
	WriteBatch(points []Point) error
}

type Point struct {
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Tags      map[string]string
}

type TSMTree struct {
	log         zerolog.Logger
	dataDir     string
	mem         *Memtable
	immutable   []*Memtable
	files       []*TSMFile
	maxMemSize  int
	mu          sync.RWMutex
	flushTicker *time.Ticker
	writeMu     sync.Mutex
	done        chan struct{}
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

type TimeRange struct {
	Start int64
	End   int64
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

func NewTSMTree(conf Config) *TSMTree {
	t := &TSMTree{
		dataDir:     conf.DataDir,
		mem:         &Memtable{data: make(map[string][]Point)},
		maxMemSize:  conf.MaxMemSize,
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(conf.FlushInterval),
	}

	t.log = zerolog.New(os.Stderr).With().Timestamp().Logger()
	t.flushTicker = time.NewTicker(conf.FlushInterval)
	go t.flushBackgroundJob()

	return t
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

func (t *TSMTree) Write(key string, timestamp int64, val float64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	p := Point{Timestamp: timestamp, Value: val}
	t.mem.data[key] = append(t.mem.data[key], p)
	t.mem.size += 16

	if t.mem.size >= t.maxMemSize {
		t.log.Info().Msg("writable memtable reached maximum capacity")
		t.immutable = append(t.immutable, t.mem)
		t.mem = &Memtable{data: make(map[string][]Point)}
	}

	return nil
}

func (t *TSMTree) WriteBatch(points []Point) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	const batchSize = 2048
	var wg sync.WaitGroup
	errorChan := make(chan error, (len(points)+batchSize-1)/batchSize)

	for i := 0; i < len(points); i += batchSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + batchSize
			if end > len(points) {
				end = len(points)
			}
			if err := t.writeBatch(points[start:end]); err != nil {
				errorChan <- err
			}
		}(i)
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

	return nil
}

func (t *TSMTree) writeBatch(points []Point) error {
	for _, p := range points {
		if err := t.Write(p.Metric, p.Timestamp, p.Value); err != nil {
			return err
		}
	}
	return nil
}

func (t *TSMTree) Read(key string, minTime, maxTime int64) ([]Point, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var res []Point
	res = append(res, t.readFromMem(key, minTime, maxTime)...)

	// Read the points from disk in a parallel fashion.
	var wg sync.WaitGroup
	resultsChan := make(chan []Point, len(t.files))
	errorChan := make(chan error, len(t.files))

	for _, file := range t.files {
		wg.Add(1)

		go func(f *TSMFile) {
			defer wg.Done()
			points, err := f.read(key, minTime, maxTime)
			if err != nil {
				errorChan <- err
				return
			}
			resultsChan <- points
		}(file)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorChan)
	}()

	for err := range errorChan {
		if err != nil {
			return nil, err
		}
	}

	for points := range resultsChan {
		res = append(res, points...)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp < res[j].Timestamp
	})

	return res, nil
}

func (t *TSMTree) readFromMem(key string, minTime, maxTime int64) []Point {
	var res []Point

	if points, ok := t.mem.data[key]; ok {
		for _, p := range points {
			if p.Timestamp >= minTime && p.Timestamp <= maxTime {
				res = append(res, p)
			}
		}
	}

	for _, immutable := range t.immutable {
		if points, ok := immutable.data[key]; ok {
			for _, p := range points {
				if p.Timestamp >= minTime && p.Timestamp <= maxTime {
					res = append(res, p)
				}
			}
		}
	}

	return res
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
		go func(mt *Memtable) {
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

func (t *TSMTree) flushMemtable(table *Memtable) error {
	t.log.Info().Msg("creating a tsm file from memtable")
	file, err := t.createTSMFile()
	if err != nil {
		return err
	}

	for key, points := range table.data {
		if err := file.write(key, points); err != nil {
			return err
		}
	}
	t.log.Info().Msg("wrote points into tsm file")

	if err := file.createIndexFile(); err != nil {
		return err
	}
	t.log.Info().Msg("created a tsm index file")

	if err := file.finalize(); err != nil {
		return err
	}
	t.log.Info().Msg("finalized tsm index file")

	t.files = append(t.files, file)
	return nil
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

func (f *TSMFile) writeKey(key string) error {
	keyLength := uint16(len(key))
	if err := binary.Write(f.file, binary.LittleEndian, keyLength); err != nil {
		return err
	}
	f.writePos += 2

	n, err := f.file.WriteString(key)
	if err != nil {
		return err
	}
	f.writePos += int64(n)

	return nil
}

func (f *TSMFile) write(key string, points []Point) error {
	if len(points) == 0 {
		return nil
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	offset := f.writePos

	// Write key
	if err := f.writeKey(key); err != nil {
		return err
	}

	// Prepare data for compression
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(points))); err != nil {
		return err
	}

	for _, p := range points {
		// write basic stuff
		if err := binary.Write(&buf, binary.LittleEndian, p.Timestamp); err != nil {
			return err
		}

		if err := binary.Write(&buf, binary.LittleEndian, p.Value); err != nil {
			return err
		}

		if err := binary.Write(&buf, binary.LittleEndian, uint8(len(p.Tags))); err != nil {
			return err
		}

		for tag, val := range p.Tags {
			if err := binary.Write(&buf, binary.LittleEndian, uint8(len(tag))); err != nil {
				return err
			}

			if _, err := buf.WriteString(tag); err != nil {
				return err
			}

			if err := binary.Write(&buf, binary.LittleEndian, uint8(len(val))); err != nil {
				return err
			}

			if _, err := buf.WriteString(val); err != nil {
				return err
			}
		}
	}

	// Compress and write data
	compressedData := snappy.Encode(nil, buf.Bytes())

	// Write the size of the compressed data first
	if err := binary.Write(f.file, binary.LittleEndian, uint32(len(compressedData))); err != nil {
		return err
	}
	f.writePos += 4

	n, err := f.file.Write(compressedData)
	if err != nil {
		return err
	}
	f.writePos += int64(n)

	blockSize := f.writePos - offset
	f.index[key] = append(f.index[key], IndexEntry{
		MinTime: points[0].Timestamp,
		MaxTime: points[len(points)-1].Timestamp,
		Offset:  offset,
		Size:    blockSize,
	})

	return nil
}

func (f *TSMFile) read(key string, minTime, maxTime int64) ([]Point, error) {
	entries, ok := f.index[key]
	if !ok {
		return nil, nil
	}

	var res []Point
	for _, entry := range entries {
		if entry.MinTime > maxTime || entry.MaxTime < minTime {
			continue
		}

		if _, err := f.file.Seek(entry.Offset, io.SeekStart); err != nil {
			return nil, err
		}

		var klen uint16
		if err := binary.Read(f.file, binary.LittleEndian, &klen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %v", err)
		}

		keyBytes := make([]byte, klen)
		if _, err := io.ReadFull(f.file, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}

		var compressedSize uint32
		if err := binary.Read(f.file, binary.LittleEndian, &compressedSize); err != nil {
			return nil, err
		}

		compressedData := make([]byte, compressedSize)
		_, err := io.ReadFull(f.file, compressedData)
		if err != nil {
			return nil, err
		}

		decompressedData, err := snappy.Decode(nil, compressedData)
		if err != nil {
			return nil, err
		}

		buf := bytes.NewReader(decompressedData)

		var numPoints uint32
		if err := binary.Read(buf, binary.LittleEndian, &numPoints); err != nil {
			return nil, err
		}

		for i := uint32(0); i < numPoints; i++ {
			var timestamp int64
			var value float64
			var tagsLen uint8

			if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
				return nil, err
			}

			if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
				return nil, err
			}

			if err := binary.Read(buf, binary.LittleEndian, &tagsLen); err != nil {
				return nil, err
			}

			tagMap := make(map[string]string, tagsLen)
			for tagIdx := uint8(0); tagIdx < tagsLen; tagIdx++ {
				var tagKeyLen, tagValLen uint8
				if err := binary.Read(buf, binary.LittleEndian, &tagKeyLen); err != nil {
					return nil, err
				}

				tagKey := make([]byte, tagKeyLen)
				if _, err := io.ReadFull(buf, tagKey); err != nil {
					return nil, err
				}

				if err := binary.Read(buf, binary.LittleEndian, &tagValLen); err != nil {
					return nil, err
				}

				tagVal := make([]byte, tagValLen)
				if _, err := io.ReadFull(buf, tagVal); err != nil {
					return nil, err
				}
				tagMap[string(tagKey)] = string(tagVal)
			}

			if timestamp >= minTime && timestamp <= maxTime {
				res = append(res, Point{Timestamp: timestamp, Value: value, Tags: tagMap})
			}
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp < res[j].Timestamp
	})

	return res, nil
}

// type TSMFile struct {
// 	path     string
// 	file     *os.File
// 	index    map[string][]IndexEntry
// 	writePos int64
// }

// type IndexEntry struct {
// 	MinTime int64
// 	MaxTime int64
// 	Offset  int64
// 	Size    int64
// }

// encodeIndex writes
func (t *TSMFile) encodeIndex() ([]byte, error) {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.LittleEndian, uint16(len(t.index))); err != nil {
		return nil, err
	}

	for metric, entries := range t.index {
		if err := binary.Write(&b, binary.LittleEndian, uint8(len(metric))); err != nil {
			return nil, err
		}

		if _, err := b.WriteString(metric); err != nil {
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
		t.index = make(map[string][]IndexEntry, indexSize)
	}

	for i := uint16(0); i < indexSize; i++ {
		var metricLength uint8
		if err := binary.Read(buf, binary.LittleEndian, &metricLength); err != nil {
			return err
		}

		metric := make([]byte, metricLength)
		if _, err := io.ReadFull(buf, metric); err != nil {
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

			indexes = append(indexes, indexEntry)
		}

		t.index[string(metric)] = indexes
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
	tsmFileChan := make(chan *TSMFile, 16)

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

			if err := tsmFile.decodeIndex(fileData); err != nil {
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
