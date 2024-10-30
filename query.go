package serie

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// query.go -- this file handles all of the query building code the lexing and parsing and the query construction from that

// Example of query language, it's quite similar to SQL by design.
//   SELECT ts, location, value FROM weather
//     WHERE ts >= '2024-01-10' AND ts <= '2024-01-25' AND location = 'HELSINKI' AND value > -10.0
//
// If we look at what a given datapoint we can relate different parts. 'weather' is the metric
// ts is the built-in name for timestamp, location and note are both tags that the metrics might have.
// and in the condition part the user the user can query based on the timestamp, tags and values.
//
// The query language will also support aggregate functions like AVG, SUM etc.
//   SELECT AVG(value), location FROM weather GROUP BY location
// Here the average temperature is measured for each location in the dataset.

type query struct {
	aggregates []string
	metric     string
	groupBy    []string // list of tags to group by
	timeStart  int64
	timeEnd    int64
}

func parseQuery(input string) (*query, error) {
	parts := strings.Fields(input)
	if len(parts) < 4 || parts[0] != "SELECT" {
		return nil, fmt.Errorf("invalid query format")
	}

	q := &query{}

	fromIndex := indexOf(parts, "FROM")
	if fromIndex == -1 {
		return nil, fmt.Errorf("missing FROM clause")
	}
	q.aggregates = parseAggregates(parts[1:fromIndex])

	if fromIndex+1 >= len(parts) {
		return nil, fmt.Errorf("missing metric")
	}
	q.metric = parts[fromIndex+1]

	betweenIndex := indexOf(parts, "BETWEEN")
	if betweenIndex != -1 {
		if betweenIndex+1 >= len(parts) {
			return nil, fmt.Errorf("invalid BETWEEN clause")
		}
		timeRange := strings.Split(parts[betweenIndex+1], ":")
		if len(timeRange) != 2 {
			return nil, fmt.Errorf("invalid time range format in BETWEEN clause")
		}

		var err error
		if timeRange[0] != "" {
			q.timeStart, err = strconv.ParseInt(timeRange[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid start timestamp: %v", err)
			}
		} else {
			q.timeStart = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
		}

		if timeRange[1] != "" {
			q.timeEnd, err = strconv.ParseInt(timeRange[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid end timestamp: %v", err)
			}
		} else {
			q.timeEnd = time.Now().Unix()
		}
	}

	groupByIndex := indexOf(parts, "GROUP")
	if groupByIndex != -1 && groupByIndex+1 < len(parts) && parts[groupByIndex+1] == "BY" {
		for _, tag := range parts[groupByIndex+2:] {
			if tag == "BETWEEN" {
				break
			}
			tag = strings.TrimSuffix(tag, ",")
			q.groupBy = append(q.groupBy, tag)
		}
	}

	return q, nil
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}

func parseAggregates(parts []string) []string {
	var aggregates []string
	for _, part := range parts {
		aggregates = append(aggregates, strings.TrimSuffix(part, ","))
	}
	return aggregates
}
