package server

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"
)

type ErrorCallback func(error)

// Options can be used to set custom options for a Server.
type Options struct {
	// The URLs used to connect to NATS servers.
	natsURLs string

	// Callbacks to execute on client errors. This is mostly used for testing.
	clientErrorCallbacks []ErrorCallback
}

func OptionsDefault() Options {
	return Options{
		natsURLs: nats.DefaultURL,
	}
}

// Option is a function on the options for a Server.
type Option func(*Options) error

// On this interval, the stats will be published.
func NATSURLs(natsURLs string) Option {
	return func(o *Options) error {
		o.natsURLs = natsURLs
		return nil
	}
}

// Callbacks to execute on client errors. This is mostly used for testing.
func ClientErrorCallbacks(callbacks ...ErrorCallback) Option {
	return func(o *Options) error {
		o.clientErrorCallbacks = append(o.clientErrorCallbacks, callbacks...)
		return nil
	}
}

type Server struct {
	opts Options

	mu       sync.Mutex
	natsConn *nats.Conn
	Mux      *http.ServeMux
}

func NewServer(options ...Option) (*Server, error) {
	opts := OptionsDefault()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	s := &Server{
		opts: opts,
	}

	natsConn, err := nats.Connect(s.opts.natsURLs)
	if err != nil {
		return nil, fmt.Errorf("NATS Connect: %w", err)
	}
	s.natsConn = natsConn

	s.Mux = http.NewServeMux()

	s.Mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "OK")
	})
	s.Mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(opts, natsConn, w, r)
	})

	return s, nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.natsConn.Close()
}

// func serveHome(w http.ResponseWriter, r *http.Request) {
// 	log.Println(r.URL)
// 	if r.URL.Path != "/" {
// 		http.Error(w, "Not found", http.StatusNotFound)
// 		return
// 	}
// 	if r.Method != "GET" {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}
// 	http.ServeFile(w, r, "home.html")
// }
