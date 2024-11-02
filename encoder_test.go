package serie

// package serie
//
// import (
// 	"fmt"
// 	"math"
// 	"reflect"
// 	"testing"
// 	"time"
// )
//
// func TestCompression(t *testing.T) {
// 	tests := []struct {
// 		name   string
// 		points []Point
// 	}{
// 		{
// 			name:   "empty points",
// 			points: []Point{},
// 		},
// 		{
// 			name: "single point",
// 			points: []Point{
// 				{
// 					Timestamp: 1000,
// 					Value:     123.45,
// 					Tags:      map[string]string{"host": "server1"},
// 				},
// 			},
// 		},
// 		{
// 			name: "regular intervals",
// 			points: []Point{
// 				{
// 					Timestamp: 1000,
// 					Value:     10.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 				{
// 					Timestamp: 2000,
// 					Value:     11.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 				{
// 					Timestamp: 3000,
// 					Value:     12.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 			},
// 		},
// 		{
// 			name: "irregular intervals",
// 			points: []Point{
// 				{
// 					Timestamp: 1000,
// 					Value:     10.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 				{
// 					Timestamp: 1500,
// 					Value:     11.5,
// 					Tags:      map[string]string{"host": "server2", "region": "us-east"},
// 				},
// 				{
// 					Timestamp: 3000,
// 					Value:     12.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 				{
// 					Timestamp: 5000,
// 					Value:     13.5,
// 					Tags:      map[string]string{"host": "server2", "region": "us-east"},
// 				},
// 			},
// 		},
// 		{
// 			name: "missing tags",
// 			points: []Point{
// 				{
// 					Timestamp: 1000,
// 					Value:     10.5,
// 					Tags:      map[string]string{"host": "server1", "region": "us-west"},
// 				},
// 				{
// 					Timestamp: 2000,
// 					Value:     11.5,
// 					Tags:      map[string]string{"host": "server1"},
// 				},
// 				{
// 					Timestamp: 3000,
// 					Value:     12.5,
// 					Tags:      map[string]string{"region": "us-west"},
// 				},
// 			},
// 		},
// 		{
// 			name: "special values",
// 			points: []Point{
// 				{
// 					Timestamp: 1000,
// 					Value:     0.0,
// 					Tags:      map[string]string{"host": "server1"},
// 				},
// 				{
// 					Timestamp: 2000,
// 					Value:     math.MaxFloat64,
// 					Tags:      map[string]string{"host": "server1"},
// 				},
// 				{
// 					Timestamp: 3000,
// 					Value:     math.SmallestNonzeroFloat64,
// 					Tags:      map[string]string{"host": "server1"},
// 				},
// 			},
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			block := NewBlock()
//
// 			compressedTimestamps := block.CompressTimestamps(tt.points)
// 			compressedValues := block.CompressValues(tt.points)
// 			compressedTags := block.CompressTags(tt.points)
//
// 			decompressed := make([]Point, len(tt.points))
//
// 			err := DecompressTimestamps(compressedTimestamps, decompressed)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress timestamps: %v", err)
// 			}
//
// 			err = DecompressValues(compressedValues, decompressed)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress values: %v", err)
// 			}
//
// 			err = DecompressTags(compressedTags, decompressed, block.td)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress tags: %v", err)
// 			}
//
// 			if len(decompressed) != len(tt.points) {
// 				t.Fatalf("Length mismatch: got %d, want %d", len(decompressed), len(tt.points))
// 			}
//
// 			for i := range tt.points {
// 				if tt.points[i].Timestamp != decompressed[i].Timestamp {
// 					t.Errorf("Point %d timestamp mismatch: got %d, want %d",
// 						i, decompressed[i].Timestamp, tt.points[i].Timestamp)
// 				}
//
// 				if math.Abs(tt.points[i].Value-decompressed[i].Value) > 1e-10 {
// 					t.Errorf("Point %d value mismatch: got %f, want %f",
// 						i, decompressed[i].Value, tt.points[i].Value)
// 				}
//
// 				if !reflect.DeepEqual(tt.points[i].Tags, decompressed[i].Tags) {
// 					t.Errorf("Point %d tags mismatch:\ngot  %v\nwant %v",
// 						i, decompressed[i].Tags, tt.points[i].Tags)
// 				}
// 			}
// 		})
// 	}
// }
//
// func TestTimestampCompression(t *testing.T) {
// 	tests := []struct {
// 		name      string
// 		intervals []int64
// 		validate  func([]Point, []Point) error
// 	}{
// 		{
// 			name:      "even intervals",
// 			intervals: []int64{60, 60, 60, 60},
// 			validate: func(original, decompressed []Point) error {
// 				for i := 1; i < len(original); i++ {
// 					delta := original[i].Timestamp - original[i-1].Timestamp
// 					if delta != 60 {
// 						return fmt.Errorf("interval %d: expected 60, got %d", i, delta)
// 					}
// 				}
// 				return nil
// 			},
// 		},
// 		{
// 			name:      "increasing intervals",
// 			intervals: []int64{60, 120, 180, 240},
// 			validate: func(original, decompressed []Point) error {
// 				for i := 1; i < len(original); i++ {
// 					delta := original[i].Timestamp - original[i-1].Timestamp
// 					if delta != int64(i*60) {
// 						return fmt.Errorf("interval %d: expected %d, got %d", i, i*60, delta)
// 					}
// 				}
// 				return nil
// 			},
// 		},
// 		{
// 			name:      "mixed intervals",
// 			intervals: []int64{60, 30, 90, 45},
// 			validate: func(original, decompressed []Point) error {
// 				expectedIntervals := []int64{60, 30, 90, 45}
// 				for i := 1; i < len(original); i++ {
// 					delta := original[i].Timestamp - original[i-1].Timestamp
// 					if delta != expectedIntervals[i-1] {
// 						return fmt.Errorf("interval %d: expected %d, got %d",
// 							i, expectedIntervals[i-1], delta)
// 					}
// 				}
// 				return nil
// 			},
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			var points []Point
// 			now := time.Now().Unix()
// 			current := now
//
// 			points = append(points, Point{Timestamp: current})
// 			for _, interval := range tt.intervals {
// 				current += interval
// 				points = append(points, Point{Timestamp: current})
// 			}
//
// 			block := NewBlock()
// 			compressed := block.CompressTimestamps(points)
// 			decompressed := make([]Point, len(points))
//
// 			err := DecompressTimestamps(compressed, decompressed)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress timestamps: %v", err)
// 			}
//
// 			if err := tt.validate(points, decompressed); err != nil {
// 				t.Errorf("Validation failed: %v", err)
// 			}
// 		})
// 	}
// }
//
// func TestValueCompression(t *testing.T) {
// 	tests := []struct {
// 		name   string
// 		values []float64
// 	}{
// 		{
// 			name:   "identical values",
// 			values: []float64{100.0, 100.0, 100.0, 100.0},
// 		},
// 		{
// 			name:   "small deltas",
// 			values: []float64{100.0, 100.1, 100.2, 100.3},
// 		},
// 		{
// 			name:   "large deltas",
// 			values: []float64{100.0, 200.0, 50.0, 150.0},
// 		},
// 		{
// 			name: "special values",
// 			values: []float64{
// 				0.0,
// 				math.MaxFloat64,
// 				math.SmallestNonzeroFloat64,
// 				-math.MaxFloat64,
// 			},
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			points := make([]Point, len(tt.values))
// 			for i, v := range tt.values {
// 				points[i] = Point{Value: v}
// 			}
//
// 			block := NewBlock()
// 			compressed := block.CompressValues(points)
// 			decompressed := make([]Point, len(points))
//
// 			err := DecompressValues(compressed, decompressed)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress values: %v", err)
// 			}
//
// 			for i := range tt.values {
// 				if math.Abs(decompressed[i].Value-tt.values[i]) > 1e-10 {
// 					t.Errorf("Value %d mismatch: got %v, want %v",
// 						i, decompressed[i].Value, tt.values[i])
// 				}
// 			}
// 		})
// 	}
// }
//
// func TestTagCompression(t *testing.T) {
// 	tests := []struct {
// 		name        string
// 		setupPoints func() []Point
// 	}{
// 		{
// 			name: "single tag",
// 			setupPoints: func() []Point {
// 				return []Point{
// 					{Tags: map[string]string{"host": "server1"}},
// 					{Tags: map[string]string{"host": "server1"}},
// 					{Tags: map[string]string{"host": "server2"}},
// 				}
// 			},
// 		},
// 		{
// 			name: "multiple tags",
// 			setupPoints: func() []Point {
// 				return []Point{
// 					{Tags: map[string]string{"host": "server1", "region": "us-west"}},
// 					{Tags: map[string]string{"host": "server2", "region": "us-east"}},
// 					{Tags: map[string]string{"host": "server1", "region": "us-west"}},
// 				}
// 			},
// 		},
// 		{
// 			name: "missing tags",
// 			setupPoints: func() []Point {
// 				return []Point{
// 					{Tags: map[string]string{"host": "server1", "region": "us-west"}},
// 					{Tags: map[string]string{"host": "server2"}},
// 					{Tags: map[string]string{"region": "us-east"}},
// 				}
// 			},
// 		},
// 		{
// 			name: "empty tags",
// 			setupPoints: func() []Point {
// 				return []Point{
// 					{Tags: map[string]string{}},
// 					{Tags: map[string]string{"host": "server1"}},
// 					{Tags: map[string]string{}},
// 				}
// 			},
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			points := tt.setupPoints()
// 			block := NewBlock()
// 			compressed := block.CompressTags(points)
// 			decompressed := make([]Point, len(points))
//
// 			err := DecompressTags(compressed, decompressed, block.td)
// 			if err != nil {
// 				t.Fatalf("Failed to decompress tags: %v", err)
// 			}
//
// 			for i := range points {
// 				if !reflect.DeepEqual(points[i].Tags, decompressed[i].Tags) {
// 					t.Errorf("Point %d tags mismatch:\ngot  %v\nwant %v",
// 						i, decompressed[i].Tags, points[i].Tags)
// 				}
// 			}
// 		})
// 	}
// }
//
// func TestCompressionSizes(t *testing.T) {
// 	points := make([]Point, 1000)
// 	now := time.Now().Unix()
//
// 	for i := range points {
// 		points[i] = Point{
// 			Timestamp: now + int64(i*60),
// 			Value:     100 + float64(i%10),
// 			Tags: map[string]string{
// 				"host":   fmt.Sprintf("server%d", i%5),
// 				"region": fmt.Sprintf("region%d", i%3),
// 				"dc":     fmt.Sprintf("dc%d", i%2),
// 			},
// 		}
// 	}
//
// 	block := NewBlock()
//
// 	compressedTimestamps := block.CompressTimestamps(points)
// 	compressedValues := block.CompressValues(points)
// 	compressedTags := block.CompressTags(points)
//
// 	t.Logf("Compression sizes for %d points:", len(points))
// 	t.Logf("  Timestamps: %d bytes (%.2f bytes/point)",
// 		len(compressedTimestamps), float64(len(compressedTimestamps))/float64(len(points)))
// 	t.Logf("  Values: %d bytes (%.2f bytes/point)",
// 		len(compressedValues), float64(len(compressedValues))/float64(len(points)))
// 	t.Logf("  Tags: %d bytes (%.2f bytes/point)",
// 		len(compressedTags), float64(len(compressedTags))/float64(len(points)))
//
// 	decompressed := make([]Point, len(points))
//
// 	err := DecompressTimestamps(compressedTimestamps, decompressed)
// 	if err != nil {
// 		t.Fatalf("Failed to decompress timestamps: %v", err)
// 	}
//
// 	err = DecompressValues(compressedValues, decompressed)
// 	if err != nil {
// 		t.Fatalf("Failed to decompress values: %v", err)
// 	}
//
// 	err = DecompressTags(compressedTags, decompressed, block.td)
// 	if err != nil {
// 		t.Fatalf("Failed to decompress tags: %v", err)
// 	}
//
// 	if !reflect.DeepEqual(points[0], decompressed[0]) {
// 		t.Error("First point mismatch")
// 	}
// 	if !reflect.DeepEqual(points[len(points)-1], decompressed[len(points)-1]) {
// 		t.Error("Last point mismatch")
// 	}
// }
