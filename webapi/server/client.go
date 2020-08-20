package server

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/internal/statspub"
	"github.com/rs/zerolog/log"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 2097152 // 2 MiB
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var (
	// Client errors
	ClientConnectionClosedError = errors.New("client connection closed")
)

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	opts Options

	closeOnce sync.Once

	mu sync.Mutex

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte

	// Shared NATS connection. Do not close this.
	natsConn *nats.Conn

	// NATS subscriptions
	subscriptions []*nats.Subscription
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Trigger a close of our send chan
		close(c.send)

		// Close the NATS subscriptions
		for i := range c.subscriptions {
			if err := c.subscriptions[i].Unsubscribe(); err != nil {
				log.Err(err).Msg("problem unsubscribing from NATS topic")
			}
		}

		c.conn.Close()
	})
}

func (c *Client) statsSubscribe(cb func(msg *nats.Msg)) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Subscribe to the NATS topic
	sub, err := c.natsConn.Subscribe(statspub.StatsSubject, cb)
	if err != nil {
		return err
	}

	// Add the subscription to the subscriptions
	c.subscriptions = append(c.subscriptions, sub)

	return nil
}

// readPump pumps messages from the websocket connection.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *Client) readPump() {
	defer func() {
		c.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		c.execClientErrorCbs(err)
	}
	c.conn.SetPongHandler(func(string) error {
		if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			c.execClientErrorCbs(err)
		}
		return nil
	})
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Err(err).Msg("unexpected close")
				c.execClientErrorCbs(err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		pm := NewProtocolMessage(c, message)
		if err := ProtocolMessageHandler(pm); err != nil {
			// TODO: Handle the errors here and respond back to user if necessary?
			log.Debug().Err(err).Msg("protocol message handler error")
			c.execClientErrorCbs(err)
		}
	}
}

// writePump pumps messages from the send buffer to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Err(err).Msg("problem setting write deadline for message")
				c.execClientErrorCbs(err)
			}
			if !ok {
				// The hub closed the channel.
				if err := c.conn.WriteMessage(websocket.CloseMessage, []byte{}); err != nil {
					log.Debug().Err(err).Msg("problem writing close websocket message")
					c.execClientErrorCbs(fmt.Errorf("problem writing close websocket message: %w", ClientConnectionClosedError))
				}
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(message); err != nil {
				// Failed to write
				log.Err(err).Msg("problem writing message")
				c.execClientErrorCbs(err)
			}

			// Add queued messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				if _, err := w.Write(newline); err != nil {
					// Failed to write
					log.Err(err).Msg("problem writing message")
					c.execClientErrorCbs(err)
				}
				if _, err := w.Write(<-c.send); err != nil {
					// Failed to write
					log.Err(err).Msg("problem writing message")
					c.execClientErrorCbs(err)
				}
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Err(err).Msg("problem setting write deadline for ticket")
				c.execClientErrorCbs(err)
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.execClientErrorCbs(err)
				// Disconnect
				return
			}
		}
	}
}

func (c *Client) execClientErrorCbs(err error) {
	for i := range c.opts.clientErrorCallbacks {
		if c.opts.clientErrorCallbacks[i] != nil {
			c.opts.clientErrorCallbacks[i](err)
		}
	}
}

// serveWs handles websocket requests from the peer.
func serveWs(opts Options, natsConn *nats.Conn, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Err(err).Msg("problem upgrading client connection")
		return
	}
	client := &Client{
		natsConn: natsConn,
		conn:     conn,
		send:     make(chan []byte, 256),
		opts:     opts,
	}

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}
