package serie

import (
	"reflect"
	"testing"
	"time"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *query
		wantErr  bool
	}{
		{
			name:  "Valid query with two aggregates, two tags, and full BETWEEN clause",
			input: "SELECT avg, sum FROM metric GROUP BY tag1, tag2 BETWEEN 1609459200:1612137600",
			expected: &query{
				aggregates: []string{"avg", "sum"},
				metric:     "metric",
				groupBy:    []string{"tag1", "tag2"},
				timeStart:  1609459200,
				timeEnd:    1612137600,
			},
			wantErr: false,
		},
		{
			name:  "Valid query with one aggregate and BETWEEN clause with only end time",
			input: "SELECT count FROM metric BETWEEN :1612137600",
			expected: &query{
				aggregates: []string{"count"},
				metric:     "metric",
				timeStart:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				timeEnd:    1612137600,
			},
			wantErr: false,
		},
		{
			name:  "Valid query with BETWEEN clause with only start time",
			input: "SELECT max FROM metric BETWEEN 1609459200:",
			expected: &query{
				aggregates: []string{"max"},
				metric:     "metric",
				timeStart:  1609459200,
				timeEnd:    time.Now().Unix(),
			},
			wantErr: false,
		},
		{
			name:  "Valid query with empty BETWEEN clause",
			input: "SELECT min FROM metric GROUP BY tag1 BETWEEN :",
			expected: &query{
				aggregates: []string{"min"},
				metric:     "metric",
				groupBy:    []string{"tag1"},
				timeStart:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				timeEnd:    time.Now().Unix(),
			},
			wantErr: false,
		},
		{
			name:  "Valid query without BETWEEN clause",
			input: "SELECT avg, sum FROM metric GROUP BY tag1, tag2",
			expected: &query{
				aggregates: []string{"avg", "sum"},
				metric:     "metric",
				groupBy:    []string{"tag1", "tag2"},
				timeStart:  0,
				timeEnd:    0,
			},
			wantErr: false,
		},
		{
			name:     "Invalid query - missing FROM",
			input:    "SELECT avg, sum metric GROUP BY tag1, tag2",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "Invalid query - missing SELECT",
			input:    "avg, sum FROM metric GROUP BY tag1, tag2",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "Invalid query - invalid BETWEEN clause format",
			input:    "SELECT avg FROM metric BETWEEN 1609459200",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "Invalid query - empty string",
			input:    "",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseQuery(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if tt.expected.timeEnd == time.Now().Unix() {
					if got.timeEnd < time.Now().Add(-time.Second).Unix() || got.timeEnd > time.Now().Unix() {
						t.Errorf("parseQuery() timeEnd = %v, want recent timestamp", got.timeEnd)
					}
					got.timeEnd = tt.expected.timeEnd
				}

				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("parseQuery() = %v, want %v", got, tt.expected)
				}
			}
		})
	}
}
