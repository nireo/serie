package serie

import (
	"log"
	"net"
	"net/http"
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
}

func (s *HttpService) read(w http.ResponseWriter, r *http.Request) {
}

func (s *HttpService) Close() error {
	return s.ln.Close()
}
