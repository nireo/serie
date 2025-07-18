package serie

import (
	"math"
	"sort"
)

func sumMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	res := make(map[string]float64)
	for i, tag := range pointTags {
		res[tag] += points[i].Value
	}
	return QueryResult{Aggregate: "sum", Result: res}, nil
}

func avgMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	sums := make(map[string]float64)
	counts := make(map[string]int)

	for i, tag := range pointTags {
		sums[tag] += points[i].Value
		counts[tag]++
	}

	res := make(map[string]float64, len(sums))
	for tag, sum := range sums {
		res[tag] = sum / float64(counts[tag])
	}
	return QueryResult{Aggregate: "avg", Result: res}, nil
}

func minMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	mins := make(map[string]float64)
	initialized := make(map[string]bool)

	for i, tag := range pointTags {
		if !initialized[tag] {
			mins[tag] = points[i].Value
			initialized[tag] = true
			continue
		}
		if points[i].Value < mins[tag] {
			mins[tag] = points[i].Value
		}
	}
	return QueryResult{Aggregate: "min", Result: mins}, nil
}

func maxMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	maxs := make(map[string]float64)
	initialized := make(map[string]bool)

	for i, tag := range pointTags {
		if !initialized[tag] {
			maxs[tag] = points[i].Value
			initialized[tag] = true
			continue
		}
		if points[i].Value > maxs[tag] {
			maxs[tag] = points[i].Value
		}
	}
	return QueryResult{Aggregate: "max", Result: maxs}, nil
}

func countMultipleTags(_ []Point, pointTags []string) (QueryResult, error) {
	counts := make(map[string]float64)
	for _, tag := range pointTags {
		counts[tag]++
	}
	return QueryResult{Aggregate: "count", Result: counts}, nil
}

func stddevMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	sums := make(map[string]float64)
	counts := make(map[string]int)
	for i, tag := range pointTags {
		sums[tag] += points[i].Value
		counts[tag]++
	}

	avgs := make(map[string]float64)
	for tag, sum := range sums {
		avgs[tag] = sum / float64(counts[tag])
	}

	squaredDiffs := make(map[string]float64)
	for i, tag := range pointTags {
		diff := points[i].Value - avgs[tag]
		squaredDiffs[tag] += diff * diff
	}

	res := make(map[string]float64)
	for tag, sqDiff := range squaredDiffs {
		res[tag] = math.Sqrt(sqDiff / float64(counts[tag]))
	}

	return QueryResult{Aggregate: "stddev", Result: res}, nil
}

func medianMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	valuesByTag := make(map[string][]float64)
	for i, tag := range pointTags {
		valuesByTag[tag] = append(valuesByTag[tag], points[i].Value)
	}

	res := make(map[string]float64)
	for tag, values := range valuesByTag {
		sort.Float64s(values)
		mid := len(values) / 2
		if len(values)%2 == 0 {
			res[tag] = (values[mid-1] + values[mid]) / 2
		} else {
			res[tag] = values[mid]
		}
	}

	return QueryResult{Aggregate: "median", Result: res}, nil
}

func percentile90MultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	valuesByTag := make(map[string][]float64)
	for i, tag := range pointTags {
		valuesByTag[tag] = append(valuesByTag[tag], points[i].Value)
	}

	res := make(map[string]float64)
	for tag, values := range valuesByTag {
		sort.Float64s(values)
		index := int(math.Ceil(0.9*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		res[tag] = values[index]
	}

	return QueryResult{Aggregate: "percentile90", Result: res}, nil
}

func rangeMultipleTags(points []Point, pointTags []string) (QueryResult, error) {
	mins := make(map[string]float64)
	maxs := make(map[string]float64)
	initialized := make(map[string]bool)

	for i, tag := range pointTags {
		if !initialized[tag] {
			mins[tag] = points[i].Value
			maxs[tag] = points[i].Value
			initialized[tag] = true
			continue
		}
		if points[i].Value < mins[tag] {
			mins[tag] = points[i].Value
		}
		if points[i].Value > maxs[tag] {
			maxs[tag] = points[i].Value
		}
	}

	res := make(map[string]float64)
	for tag := range mins {
		res[tag] = maxs[tag] - mins[tag]
	}

	return QueryResult{Aggregate: "range", Result: res}, nil
}

func sum(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sum(values) / float64(len(values))
}

func min(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func max(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 0 {
		return (values[mid-1] + values[mid]) / 2
	}
	return values[mid]
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	index := int(math.Ceil((p / 100) * float64(len(values))))
	return values[index-1]
}

func standardDeviation(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	avg := average(values)
	var sumSquares float64
	for _, v := range values {
		sumSquares += (v - avg) * (v - avg)
	}
	return math.Sqrt(sumSquares / float64(len(values)))
}

func rangeValue(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return max(values) - min(values)
}
