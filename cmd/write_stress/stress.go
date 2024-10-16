package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/nireo/serie"
)

const (
	numMetrics     = 100
	numDataPoints  = 100000000
	numConcurrent  = 10
	readConcurrent = 5
	timeRange      = 24 * time.Hour
)

func main() {
	// Set up the TSMTree
	config := serie.DefaultConfig()
	config.DataDir = "./stress_test_data"
	tree := serie.NewTSMTree(config)
	defer tree.Close()

	os.MkdirAll(config.DataDir, 0755)

	fmt.Println("Writing data...")
	writeData(tree)

	fmt.Println("Reading and verifying data...")
	readAndVerifyData(tree)

	fmt.Println("Stress test completed successfully!")
}

func writeData(tree *serie.TSMTree) {
	var wg sync.WaitGroup
	pointsChan := make(chan serie.Point, numConcurrent)

	// Start worker goroutines
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for point := range pointsChan {
				if err := tree.Write(point.Metric, point.Timestamp, point.Value); err != nil {
					fmt.Printf("Error writing point: %v\n", err)
				}
			}
		}()
	}

	// Generate and send points
	baseTime := time.Now().Add(-timeRange)
	for i := 0; i < numDataPoints; i++ {
		metric := fmt.Sprintf("metric_%d", rand.Intn(numMetrics))
		timestamp := baseTime.Add(time.Duration(rand.Int63n(int64(timeRange)))).UnixNano()
		value := rand.Float64() * 100

		pointsChan <- serie.Point{
			Metric:    metric,
			Timestamp: timestamp,
			Value:     value,
		}
	}

	close(pointsChan)
	wg.Wait()
}

func readAndVerifyData(tree *serie.TSMTree) {
	var wg sync.WaitGroup
	baseTime := time.Now().Add(-timeRange)
	endTime := time.Now()

	for i := 0; i < readConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numMetrics; j++ {
				metric := fmt.Sprintf("metric_%d", j)
				points, err := tree.Read(metric, baseTime.UnixNano(), endTime.UnixNano())
				if err != nil {
					fmt.Printf("Error reading metric %s: %v\n", metric, err)
					continue
				}
				fmt.Printf("Read %d points for metric %s\n", len(points), metric)

				for k := 1; k < len(points); k++ {
					if points[k].Timestamp < points[k-1].Timestamp {
						fmt.Printf("Error: Timestamps out of order for metric %s\n", metric)
						break
					}
				}
			}
		}()
	}

	wg.Wait()
}
