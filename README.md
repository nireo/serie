# serie

> A persistant embeddable time series database written in Go.

## Features

- A LSM tree inspired time-structured merge tree to efficiently query time series data.
- HTTP api for writing and reading data
- Support for tags and querying based on tags
- Distributed using consistent hashing

## Project structure

Each file contains their own tests and each file also contains a high-level overview of the code.

- `http.go` exposes a http interface for the data engine.
- `engine.go` implements a LSM-tree database modified to better fit time series data.
- `query.go` implements a lexer and custom query language that can be used to query time series data.
- `encoding.go` contains some code to enconde entries reducing the space they take on disk.


## Configuration

The `NewTSMTree` function takes in a config that has the following parameters:

- `MaxMemSize`: The maximum size of in-memory strage before flushing to disk. This will be a bit more than the TSM files are on disk since the points are snappy compressed.
- `DataDir`: The directory for storing the given data.
- `FlushInternal`: Internal for background flushing of data to disk.

## Usage
```go
func main() {
	config := serie.DefaultConfig()
	config.DataDir = "./data"  // Set the directory for storing data files
	config.MaxMemSize = 1024 * 1024 * 5  // Set max memory size to 5MB
	config.FlushInterval = time.Minute * 5  // Set flush interval to 5 minutes

	db, err := serie.NewTSMTree(config)
	if err != nil {
		log.Fatalf("Failed to create TSMTree: %v", err)
	}
	defer db.Close()

	// Create an HTTP service
	httpService := serie.NewHttpService(":8080", db)
	err = httpService.Start()
	if err != nil {
		log.Fatalf("Failed to start HTTP service: %v", err)
	}
	defer httpService.Close()

	// Write some sample data
	points := []serie.Point{
		{
			Metric:    "temperature",
			Value:     23.5,
			Timestamp: time.Now().Unix(),
			Tags:      map[string]string{"location": "living_room", "sensor": "thermometer_1"},
		},
		{
			Metric:    "humidity",
			Value:     45.2,
			Timestamp: time.Now().Unix(),
			Tags:      map[string]string{"location": "living_room", "sensor": "hygrometer_1"},
		},
	}

	err = db.WriteBatch(points)
	if err != nil {
		log.Printf("Failed to write batch: %v", err)
	}

	// Query timestamps in the last hour
	endTime := time.Now().Unix()
	startTime := endTime - 3600

	readPoints, err := db.Read("temperature", startTime, endTime)
	if err != nil {
		log.Printf("Failed to read data: %v", err)
	} else {
		fmt.Println("Read points:", readPoints)
	}

	select {} // Keep the program running for the http service.
}
```
