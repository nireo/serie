package main

// space_analyzer.go is used such that I can check the effects of different memory saving.

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nireo/serie"
)

var (
	numMetrics     int
	numDataPoints  int
	numConcurrent  int
	readConcurrent int
	timeRange      time.Duration
	dataDir        string
)

func init() {
	flag.IntVar(&numMetrics, "metrics", 100, "Number of metrics to generate")
	flag.IntVar(&numDataPoints, "points", 1000000, "Total number of data points to write")
	flag.IntVar(&numConcurrent, "write-concurrency", 10, "Number of concurrent write goroutines")
	flag.IntVar(&readConcurrent, "read-concurrency", 5, "Number of concurrent read goroutines")
	flag.DurationVar(&timeRange, "time-range", 24*time.Hour, "Time range for data points")
	flag.StringVar(&dataDir, "data-dir", "./space_analyzer", "Directory to store TSMTree data")
}

func main() {
	flag.Parse()
	config := serie.DefaultConfig()
	config.DataDir = dataDir
	tree := serie.NewTSMTree(config)

	os.MkdirAll(config.DataDir, 0755)

	fmt.Println("Writing data...")
	writeData(tree)

	fmt.Println("Reading and verifying data...")
	readAndVerifyData(tree)

	fmt.Println("Calculating file sizes")

	tree.Close()
	files, err := getFileSizes(dataDir)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Sort files by size in descending order
	sort.Slice(files, func(i, j int) bool {
		return files[i].Size > files[j].Size
	})

	// Print file sizes
	for _, file := range files {
		fmt.Printf("%s: %s\n", file.Path, formatSize(file.Size))
	}

	// Print total size
	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	fmt.Printf("\nTotal size: %s\n", formatSize(totalSize))
}

func writeData(tree *serie.TSMTree) {
	var wg sync.WaitGroup
	pointsChan := make(chan serie.Point, numConcurrent)

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

type FileInfo struct {
	Path string
	Size int64
}

func getFileSizes(dir string) ([]FileInfo, error) {
	var files []FileInfo

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, FileInfo{Path: path, Size: info.Size()})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func formatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
