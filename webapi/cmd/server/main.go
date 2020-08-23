package main

import (
	"flag"
	"net/http"

	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/nickpoorman/nats-requeue/webapi/server"
	"github.com/rs/zerolog/log"
)

var addr = flag.String("addr", ":8080", "http service address")
var natsURLs = flag.String("nats-servers", requeue.DefaultNatsServers, "The nats server URLs (separated by comma)")

func main() {
	flag.Parse()

	s, err := server.NewServer(server.NATSURLs(*natsURLs))
	if err != nil {
		log.Fatal().Err(err).Msg("problem starting webapi server")
	}
	defer s.Close()

	log.Info().Msgf("Starting webapi server: %s", *addr)
	if err := http.ListenAndServe(*addr, logRequest(s.Mux)); err != nil {
		log.Err(err).Msg("webapi stopped")
	}
	log.Err(err).Msg("webapi shutting down")
}

func logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL)
		log.Info().Str("remote_addr", r.RemoteAddr).Str("method", r.Method).Str("url", r.URL.String()).Send()
		handler.ServeHTTP(w, r)
	})
}
