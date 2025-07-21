package serie

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// StringPool is a structure that stores strings with an id such that less string are stored
// overall. This is due to the fact that time series data often is really redundant so instead
// storing a string which has the content + length data which is n+8 bytes we store just 4 bytes as
// pointer to a string. In this case the map takes roughly 24 bytes of overhead per string and the
// byID slice takes 24 bytes. This means that if a string is repeated thousands of times the saving is
//
// with string pool: 24 + 24 + 4 * n = 48+4*n
// without: 16*n
// so for 48+4*n<16*n n>4 and metrics are often duplicated alot
type StringPool struct {
	mu      sync.RWMutex
	strings map[string]uint32
	byID    []string
	nextID  uint32
}

// NewStringPool creates a new string pool for this
func NewStringPool() *StringPool {
	return &StringPool{
		strings: make(map[string]uint32),
		byID:    make([]string, 0),
		nextID:  1,
	}
}

type PersistantStringPool struct {
	sp        *StringPool
	file      *os.File
	fileMutex sync.Mutex
}

// NewPersistantStringPool creates a way to hold string information on disk.
func NewPersistantStringPool(path string) (*PersistantStringPool, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &PersistantStringPool{
		sp:   NewStringPool(),
		file: file,
	}, nil
}

func (p *PersistantStringPool) Close() {
	if p.file != nil {
		p.file.Close()
	}
}

func (p *PersistantStringPool) writeToFile(s string, idx uint32) error {
	data := make([]byte, 0, 2+int(idx)+len(s))
	data = binary.LittleEndian.AppendUint16(data, uint16(len(s)))
	data = append(data, []byte(s)...)
	data = binary.LittleEndian.AppendUint32(data, idx)

	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	_, err := p.file.Write(data)
	return err
}

func (p *PersistantStringPool) Add(s string) (uint32, error) {
	id, isNew := p.sp.addWithStatus(s)
	if isNew {
		return id, p.writeToFile(s, id)
	}
	return id, nil
}

func (sp *StringPool) addWithStatus(s string) (uint32, bool) {
	sp.mu.RLock()
	if id, exists := sp.strings[s]; exists {
		sp.mu.RUnlock()
		return id, false
	}
	sp.mu.RUnlock()

	sp.mu.Lock()
	defer sp.mu.Unlock()

	if id, exists := sp.strings[s]; exists {
		return id, false
	}

	// Actually add the new string
	id := sp.nextID
	sp.nextID++
	sp.strings[s] = id
	sp.byID = append(sp.byID, s)
	return id, true
}

// Add adds a string to the pool and returns the id for the string. This function locks the pool
func (sp *StringPool) Add(s string) uint32 {
	id, _ := sp.addWithStatus(s)
	return id
}

// Get finds a string with a given id. causes read lock.
func (sp *StringPool) Get(id uint32) string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	if id == 0 || int(id-1) >= len(sp.byID) {
		return ""
	}
	return sp.byID[id-1]
}

func LoadPersistantStringPool(path string) (*PersistantStringPool, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return NewPersistantStringPool(path)
		}
		return nil, err
	}

	data, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	sp := NewStringPool()

	offset := 0
	maxID := uint32(0)

	for offset < len(data) {
		if offset+2 > len(data) {
			file.Close()
			return nil, fmt.Errorf("invalid file format: incomplete length field at offset %d", offset)
		}

		strLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(strLen) > len(data) {
			file.Close()
			return nil, fmt.Errorf("invalid file format: incomplete string at offset %d", offset)
		}

		str := string(data[offset : offset+int(strLen)])
		offset += int(strLen)

		if offset+4 > len(data) {
			file.Close()
			return nil, fmt.Errorf("invalid file format: incomplete ID field at offset %d", offset)
		}

		id := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if id == 0 {
			file.Close()
			return nil, fmt.Errorf("invalid ID 0 found in file at string %q", str)
		}

		if id > maxID {
			maxID = id
		}

		for len(sp.byID) < int(id) {
			sp.byID = append(sp.byID, "")
		}

		if sp.byID[id-1] != "" && sp.byID[id-1] != str {
			file.Close()
			return nil, fmt.Errorf("duplicate ID %d found with different strings: %q and %q", id, sp.byID[id-1], str)
		}

		sp.strings[str] = id
		sp.byID[id-1] = str
	}

	sp.nextID = maxID + 1
	if len(sp.strings) != len(sp.byID) {
		file.Close()
		return nil, fmt.Errorf("inconsistent state: %d strings in map, %d in slice", len(sp.strings), len(sp.byID))
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &PersistantStringPool{
		sp:   sp,
		file: file,
	}, nil
}
