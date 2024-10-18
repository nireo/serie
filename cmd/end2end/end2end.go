package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/nireo/serie"
)

const (
	numMetrics    = 10
	numWriters    = 3
	numReaders    = 10
	writeDuration = 30 * time.Second
	readInterval  = 2 * time.Second
	batchSize     = 256
	writeInterval = 100 * time.Millisecond
	flushInterval = 5 * time.Second
)

func main() {
	config := serie.DefaultConfig()
	config.DataDir = "./data"
	config.MaxMemSize = 256 * 512
	config.FlushInterval = flushInterval
	tree, err := serie.NewTSMTree(config)
	if err != nil {
		log.Fatalln("failed to setup tsm tree ", err)
	}
	defer tree.Close()
	defer os.RemoveAll(config.DataDir)

	done := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go writer(tree, i, done, &wg)
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go reader(tree, i, done, &wg)
	}

	time.Sleep(writeDuration)
	close(done)

	wg.Wait()
	fmt.Println("Concurrent operations completed.")
}

func writer(tree *serie.TSMTree, id int, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-done:
			return
		default:
			points := generateBatch()
			err := tree.WriteBatch(points)
			if err != nil {
				fmt.Printf("Writer %d error: %v\n", id, err)
			} else {
				fmt.Printf("Writer %d wrote %d points\n", id, len(points))
			}
			time.Sleep(writeInterval)
		}
	}
}

func reader(tree *serie.TSMTree, id int, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(readInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			metric := fmt.Sprintf("metric_%d", rand.Intn(numMetrics))
			endTime := time.Now().UnixNano()
			startTime := endTime - int64(5*time.Minute)

			points, err := tree.Read(metric, startTime, endTime)
			if err != nil {
				fmt.Printf("Reader %d error: %v\n", id, err)
			} else {
				fmt.Printf("Reader %d read %d points for %s\n", id, len(points), metric)
			}
		}
	}
}

func generateBatch() []serie.Point {
	points := make([]serie.Point, batchSize)
	now := time.Now().UnixNano()

	for i := 0; i < batchSize; i++ {
		points[i] = serie.Point{
			Metric:    fmt.Sprintf("metric_%d", rand.Intn(numMetrics)),
			Value:     rand.Float64() * 100,
			Timestamp: now - int64(i*int(time.Second)),
			Tags: map[string]string{
				"tag1": fmt.Sprintf("value%d", rand.Intn(5)),
				"tag2": fmt.Sprintf("value%d", rand.Intn(5)),
			},
		}
	}

	return points
}
