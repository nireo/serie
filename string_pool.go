package serie

import "sync"

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

// Add adds a string to the pool and returns the id for the string. This function locks the pool
func (sp *StringPool) Add(s string) uint32 {
	sp.mu.RLock()
	if id, exists := sp.strings[s]; exists {
		sp.mu.RUnlock()
		return id
	}
	sp.mu.RUnlock()

	sp.mu.Lock()
	defer sp.mu.Unlock()

	if id, exists := sp.strings[s]; exists {
		return id
	}

	// Actually add the new string
	id := sp.nextID
	sp.nextID++
	sp.strings[s] = id
	sp.byID = append(sp.byID, s)
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
