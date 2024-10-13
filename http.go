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
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *HttpService) write(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "unrecognized method", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading request body", http.StatusBadRequest)
		return
	}

	var points []Point
	err = json.Unmarshal(body, &points)
	if err != nil {
		http.Error(w, "error parsing json", http.StatusBadRequest)
		return
	}

	err = s.db.WriteBatch(points)
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

	points, err := s.db.Read(metric, tsStartInt, tsEndInt)
	if err != nil {
		http.Error(w, fmt.Sprintf("error reading data points: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(points)
	w.WriteHeader(http.StatusOK)
}

func (s *HttpService) Close() error {
	return s.ln.Close()
}
