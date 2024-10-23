package serie

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/soheilhy/cmux"
)

// MultiplexedService allows to have a HTTP service and RPC on the same listener. This simplifies other
// operations as the same address is always used. TODO: look into having two different listeners on different
// ports.
type MultiplexedService struct {
	baseAddr    string
	httpService *HttpService
	distributor *Distributor
	listener    net.Listener
}

func NewMultiplexedService(baseAddr string, httpService *HttpService, distributor *Distributor) *MultiplexedService {
	return &MultiplexedService{
		baseAddr:    baseAddr,
		httpService: httpService,
		distributor: distributor,
	}
}

func (ms *MultiplexedService) Start() error {
	l, err := net.Listen("tcp", ms.baseAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	ms.listener = l

	multiplexer := cmux.New(ms.listener)
	httpL := multiplexer.Match(cmux.HTTP1Fast())
	trpcL := multiplexer.Match(cmux.Any())

	rpcServer := rpc.NewServer()
	err = rpcServer.Register(ms.distributor)
	if err != nil {
		return fmt.Errorf("failed to register rpc service: %v", err)
	}

	go func() {
		rpcServer.Accept(trpcL)
	}()
	httpServer := &http.Server{
		Handler: ms.httpService,
	}

	go func() {
		err := httpServer.Serve(httpL)
		if err != nil && err != cmux.ErrListenerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	go func() {
		err := multiplexer.Serve()
		if err != nil && err != cmux.ErrListenerClosed {
			log.Printf("Multiplexer error: %v", err)
		}
	}()

	return nil
}

func (ms *MultiplexedService) Close() error {
	if err := ms.httpService.Close(); err != nil {
		return err
	}

	return ms.listener.Close()
}
