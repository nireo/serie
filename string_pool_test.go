package serie

import (
	"math/rand"
	"strings"
	"testing"
)

func TestStringPool(t *testing.T) {
	n := 10000
	alphabet := "abcdefgijklmnopqrstuwyzABCDEFGIJKLMNOPRQRSTUWYZ"
	strs := make([]string, n)
	ids := make([]uint32, n)
	sp := NewStringPool()

	for i := range n {
		var sb strings.Builder
		for range 20 {
			sb.WriteByte(alphabet[int(rand.Int31n(int32(len(alphabet))))])
		}
		id := sp.Add(sb.String())

		strs[i] = sb.String()
		ids[i] = id
	}

	// when writing an existing string the same id should be added
	for i, s := range strs {
		strid := ids[i]
		newID := sp.Add(s)

		if strid != newID {
			t.Fatalf("new id generated for string %s old id=%d new id=%d", s, strid, newID)
		}
	}

	for i, id := range ids {
		s := sp.Get(id)
		if s == "" {
			t.Fatal("string not found")
		}

		if strs[i] != s {
			t.Fatalf("mismatch strings %s | %s", s, strs[i])
		}
	}
}
