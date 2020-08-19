package webapi

import (
	"flag"
	"log"
	"net/http"

	requeue "github.com/nickpoorman/nats-requeue"
)

var addr = flag.String("addr", ":8080", "http service address")
var natsURLs = flag.String("nats-servers", requeue.DefaultNatsServers, "The nats server URLs (separated by comma)")

func main() {
	flag.Parse()

	mux, err := RunApp(*natsURLs)
	if err != nil {
		log.Fatal("RunApp: ", err)
	}
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
