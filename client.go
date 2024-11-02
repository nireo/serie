package serie

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Here the client can be used as an interface to interact with the distributed nodes.
type Client struct {
	distributor *Distributor
}

func (c *Client) Write(points []Point) error {
	if len(points) == 0 {
		return nil
	}

	// Get servers that should be written to.
	nodes, err := c.distributor.ChooseNodes(points[0].Metric)
	if err != nil {
		return err
	}

	// Encode the points once since the same data is sent to each server.
	var buf bytes.Buffer
	if err = json.NewEncoder(&buf).Encode(points); err != nil {
		return err
	}

	errChan := make(chan error)
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			errChan <- c.writeToNode(addr, &buf)
		}(node.String())
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// writeToNode writes the json encoded points to a http service.
// TODO: maybe consider having some other encoding format since json is not space efficient
// nor fast.
func (c *Client) writeToNode(addr string, data *bytes.Buffer) error {
	url := fmt.Sprintf("http://%s/write", addr)

	req, err := http.NewRequest("POST", url, data)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
