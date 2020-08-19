package webapi

import (
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
)

func RunApp(natsURLs string) (*http.ServeMux, error) {
	natsConn, err := nats.Connect(natsURLs)
	if err != nil {
		return nil, fmt.Errorf("NATS Connect: %w", err)
	}

	mux := http.NewServeMux()

	// http.HandleFunc("/", serveHome)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(natsConn, w, r)
	})

	return mux, nil
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
