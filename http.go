package serie

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type HttpService struct {
	ln    net.Listener
	addr  string
	pprof bool
	db    Engine
}

func NewHttpService(addr string, db Engine) *HttpService {
	return &HttpService{
		addr: addr,
		db:   db,
	}
}

func (s *HttpService) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Printf("serving http service failed: %s", err)
		}
	}()

	return nil
}

func (s *HttpService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/write"):
		s.write(w, r)
	case strings.HasPrefix(r.URL.Path, "/read"):
		s.read(w, r)
	case strings.HasPrefix(r.URL.Path, "/query"):
		s.query(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

type writeBody struct {
	Timestamps []int64           `json:"timestamps"`
	Values     []float64         `json:"values"`
	Tags       map[string]string `json:"tags"`
	Metric     string            `json:"metric"`
}

func (s *HttpService) write(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "unrecognized method", http.StatusMethodNotAllowed)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading request body", http.StatusBadRequest)
		return
	}

	var body writeBody
	err = json.Unmarshal(data, &body)
	if err != nil {
		http.Error(w, "error parsing json", http.StatusBadRequest)
		return
	}

	err = s.db.WriteBatch(body.Metric, body.Timestamps, body.Values, body.Tags)
	if err != nil {
		http.Error(w, "error writing data points", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *HttpService) read(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "unrecognized method", http.StatusMethodNotAllowed)
		return
	}

	qr := r.URL.Query()
	metric := qr.Get("metric")
	tsStart := qr.Get("start")
	tsEnd := qr.Get("end")

	// take all key value pairs in the url as tags. this way we don't actually have to read
	// any body or something like that.
	tags := make(map[string]string)
	for key, values := range qr {
		if key == "metric" || key == "start" || key == "end" {
			continue
		}

		if len(values) > 0 {
			tags[key] = values[0]
		}
	}

	tsStartInt, err := strconv.ParseInt(tsStart, 10, 64)
	if err != nil {
		http.Error(w, "timestamp start was not a number", http.StatusBadRequest)
		return
	}

	tsEndInt, err := strconv.ParseInt(tsEnd, 10, 64)
	if err != nil {
		http.Error(w, "timestamp end was not a number", http.StatusBadRequest)
		return
	}

	rr, err := s.db.Read(metric, tsStartInt, tsEndInt, tags)
	if err != nil {
		http.Error(w, fmt.Sprintf("error reading data points: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(rr)
	w.WriteHeader(http.StatusOK)
}

func (s *HttpService) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "unrecognized method", http.StatusMethodNotAllowed)
		return
	}

	qr := r.URL.Query()
	queryString := qr.Get("query")

	queryRes, err := s.db.Query(queryString)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to query results: %s", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(queryRes)
}

func (s *HttpService) Close() error {
	return s.ln.Close()
}
