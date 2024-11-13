package serie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/rs/zerolog"
)

// Here the client can be used as an interface to interact with the distributed nodes.
type Client struct {
	distributor *Distributor
	log         zerolog.Logger
}

func NewClient(d *Distributor) *Client {
	return &Client{
		distributor: d,
		log:         zerolog.New(os.Stderr).With().Timestamp().Str("component", "client").Logger(),
	}
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

	c.log.Info().Str("nodes", fmt.Sprintf("%+v", nodes))

	data, err := json.Marshal(points)
	if err != nil {
		return err
	}

	errChan := make(chan error)
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			buf := bytes.NewBuffer(data)
			errChan <- c.writeToNode(addr, buf)
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

func (c *Client) Query(query string) ([]QueryResult, error) {
	// read from the nodes one by one such that the first nodes is checked first
	// and after that try for each node sequentially to ensure that we're reading
	// from the main node.

	parsedQuery, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	nodes, err := c.distributor.ChooseNodes(parsedQuery.metric)
	if err != nil {
		return nil, err
	}

	c.log.Info().Str("nodes", fmt.Sprintf("%+v", nodes))

	for _, node := range nodes {
		urlValues := url.Values{}
		urlValues.Add("query", query)
		url := fmt.Sprintf("http://%s/query", node.String())

		fullUrl := fmt.Sprintf("%s?%s", url, urlValues.Encode())

		// if we error here we have some other problems so just quit
		req, err := http.NewRequest(http.MethodGet, fullUrl, nil)
		if err != nil {
			return nil, err
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			// try to get nodes from other node in list.
			continue
		}
		defer resp.Body.Close()

		fmt.Fprintf(os.Stderr, "got status %d\n", resp.StatusCode)
		if resp.StatusCode != http.StatusOK {
			continue
		}

		var queryResult []QueryResult
		if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
			fmt.Fprintf(os.Stderr, "got error encoding %s\n", err)
			continue
		}

		return queryResult, nil
	}

	return nil, errors.New("cannot connect to any nodes")
}
