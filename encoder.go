package serie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

type TagDictionary struct {
	stringToID map[string]uint16
	idToString map[uint16]string
	nextID     uint16
}

func NewTagDictionary() *TagDictionary {
	return &TagDictionary{
		stringToID: make(map[string]uint16),
		idToString: make(map[uint16]string),
		nextID:     0,
	}
}

// GetID returns a given tags ID and creates one for the tag if it doesn't exist.
func (td *TagDictionary) GetID(s string) uint16 {
	if id, exists := td.stringToID[s]; exists {
		return id
	}

	id := td.nextID
	td.stringToID[s] = id
	td.idToString[id] = s
	td.nextID++

	return id
}

// GetString finds a given tag's string with a id.
func (td *TagDictionary) GetString(id uint16) string {
	return td.idToString[id]
}

// Block represents a compressed block of time series data
type Block struct {
	td *TagDictionary
}

func NewBlock() *Block {
	return &Block{
		td: NewTagDictionary(),
	}
}

func (b *Block) CompressTimestamps(points []Point) []byte {
	if len(points) == 0 {
		return nil
	}

	var buf bytes.Buffer
	varintBuf := make([]byte, binary.MaxVarintLen64)

	binary.Write(&buf, binary.LittleEndian, points[0].Timestamp)
	if len(points) == 1 {
		return buf.Bytes()
	}

	prevDelta := points[1].Timestamp - points[0].Timestamp
	n := binary.PutVarint(varintBuf, prevDelta)
	buf.Write(varintBuf[:n])

	for i := 2; i < len(points); i++ {
		delta := points[i].Timestamp - points[i-1].Timestamp
		doubleDelta := delta - prevDelta
		n := binary.PutVarint(varintBuf, doubleDelta)
		buf.Write(varintBuf[:n])
		prevDelta = delta
	}

	return buf.Bytes()
}

func (b *Block) CompressValues(points []Point) []byte {
	if len(points) == 0 {
		return nil
	}

	var buf bytes.Buffer
	varintBuf := make([]byte, binary.MaxVarintLen64)

	binary.Write(&buf, binary.LittleEndian, points[0].Value)

	prevVal := points[0].Value
	for i := 1; i < len(points); i++ {
		currVal := points[i].Value
		delta := currVal - prevVal

		bits := math.Float64bits(delta)
		n := binary.PutUvarint(varintBuf, bits)
		buf.Write(varintBuf[:n])

		prevVal = currVal
	}

	return buf.Bytes()
}

func (b *Block) CompressTags(points []Point) []byte {
	if len(points) == 0 {
		return nil
	}

	tagKeys := make(map[string]struct{})
	for _, p := range points {
		for k := range p.Tags {
			tagKeys[k] = struct{}{}
		}
	}

	var buf bytes.Buffer
	varintBuf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutUvarint(varintBuf, uint64(len(tagKeys)))
	buf.Write(varintBuf[:n])

	for tagKey := range tagKeys {
		keyID := b.td.GetID(tagKey)
		n = binary.PutUvarint(varintBuf, uint64(keyID))
		buf.Write(varintBuf[:n])

		n = binary.PutUvarint(varintBuf, uint64(len(points)))
		buf.Write(varintBuf[:n])

		for _, p := range points {
			value, exists := p.Tags[tagKey]
			if !exists {
				value = ""
			}
			valueID := b.td.GetID(value)
			n = binary.PutUvarint(varintBuf, uint64(valueID))
			buf.Write(varintBuf[:n])
		}
	}

	return buf.Bytes()
}

func DecompressTimestamps(data []byte, points []Point) error {
	if len(data) == 0 || len(points) == 0 {
		return nil
	}

	buf := bytes.NewReader(data)

	var firstTimestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &firstTimestamp); err != nil {
		return fmt.Errorf("reading first timestamp: %w", err)
	}
	points[0].Timestamp = firstTimestamp

	if len(points) == 1 {
		return nil
	}

	firstDelta, err := binary.ReadVarint(buf)
	if err != nil {
		return fmt.Errorf("reading first delta: %w", err)
	}

	points[1].Timestamp = firstTimestamp + firstDelta
	prevTimestamp := points[1].Timestamp
	prevDelta := firstDelta

	for i := 2; i < len(points); i++ {
		doubleDelta, err := binary.ReadVarint(buf)
		if err != nil {
			return fmt.Errorf("reading delta at position %d: %w", i, err)
		}

		delta := prevDelta + doubleDelta
		timestamp := prevTimestamp + delta

		points[i].Timestamp = timestamp
		prevTimestamp = timestamp
		prevDelta = delta
	}

	return nil
}

func DecompressValues(data []byte, points []Point) error {
	if len(data) == 0 || len(points) == 0 {
		return nil
	}

	buf := bytes.NewReader(data)

	var firstValue float64
	if err := binary.Read(buf, binary.LittleEndian, &firstValue); err != nil {
		return fmt.Errorf("reading first value: %w", err)
	}
	points[0].Value = firstValue

	prevValue := firstValue
	for i := 1; i < len(points); i++ {
		bits, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("reading value at position %d: %w", i, err)
		}

		delta := math.Float64frombits(bits)
		currentValue := prevValue + delta
		points[i].Value = currentValue
		prevValue = currentValue
	}

	return nil
}

func DecompressTags(data []byte, points []Point, td *TagDictionary) error {
	if len(data) == 0 || len(points) == 0 {
		return nil
	}

	buf := bytes.NewReader(data)

	numTagKeys, err := binary.ReadUvarint(buf)
	if err != nil {
		return fmt.Errorf("reading number of tag keys: %w", err)
	}

	for i := uint64(0); i < numTagKeys; i++ {
		keyID, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("reading tag key ID: %w", err)
		}
		tagKey := td.GetString(uint16(keyID))

		numPoints, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("reading number of points for tag %s: %w", tagKey, err)
		}

		if numPoints != uint64(len(points)) {
			return fmt.Errorf("points count mismatch: got %d, want %d", numPoints, len(points))
		}

		for j := uint64(0); j < numPoints; j++ {
			valueID, err := binary.ReadUvarint(buf)
			if err != nil {
				return fmt.Errorf("reading tag value ID at position %d: %w", j, err)
			}

			if points[j].Tags == nil {
				points[j].Tags = make(map[string]string)
			}

			value := td.GetString(uint16(valueID))
			if value != "" {
				points[j].Tags[tagKey] = value
			}
		}
	}

	return nil
}
