package requeue

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	badger "github.com/dgraph-io/badger/v2"
)

const (
	// DefaultNatsServers is the default nats server URLs (separated by comma).
	DefaultNatsServers = nats.DefaultURL

	// DefaultNatsClientName is the default name to assign to the NATS client
	// connection.
	DefaultNatsClientName = "requeue-nats"

	// DefaultNatsRetryOnFailure is true by default so that requeue will attempt
	// to automatically reconnect to nats on a failure.
	DefaultNatsRetryOnFailure = true

	// DefaultNatsSubject is the deafult subject requeue will subscribe to for
	// messages. By default `requeue.>` will match
	// `requeue.foo`, `requeue.foo.bar`, and `requeue.foo.bar.baz`.
	// ">" matches any length of the tail of a subject, and can only be the last token
	// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'.
	DefaultNatsSubject = "requeue.>"

	// DefaultNatsQueueName is the default queue to subscribe to. Messages from
	// the queue will be distributed amongst the the subscribers of the queue.
	DefaultNatsQueueName = "requeue-workers"
)

func Connect(options ...Option) (*Conn, error) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}
	return opts.Connect()
}

// Option is a function on the options to connect a Service.
type Option func(*Options) error

// ConnectContext sets the context to be used for connect.
func ConnectContext(ctx context.Context) Option {
	return func(o *Options) error {
		o.ctx = ctx
		return nil
	}
}

// NATSServers is the nats server URLs (separated by comma).
func NATSServers(natsServers string) Option {
	return func(o *Options) error {
		o.NatsServers = natsServers
		return nil
	}
}

// NATSSubject is the subject requeue will subscribe to for
// messages. By default `requeue.>` will match
// `requeue.foo`, `requeue.foo.bar`, and `requeue.foo.bar.baz`.
// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'.
func NATSSubject(natsSubject string) Option {
	return func(o *Options) error {
		o.NatsSubject = natsSubject
		return nil
	}
}

// NatsQueueName is the queue to subscribe to. Messages from the queue will be
// distributed amongst the the subscribers of the queue.
func NATSQueueName(natsQueueName string) Option {
	return func(o *Options) error {
		o.NatsQueueName = natsQueueName
		return nil
	}
}

// NATSOptions are options that will be provided to NATS upon establishing a
// connection.
func NATSOptions(natsOptions []nats.Option) Option {
	return func(o *Options) error {
		o.NatsOptions = natsOptions
		return nil
	}
}

// NATSConnectionError is a callback when the connection is unable to be
// established.
func NATSConnectionError(connErrCb func(*Conn, error)) Option {
	return func(o *Options) error {
		o.NatsConnErrCB = connErrCb
		return nil
	}
}

// BadgerDataPath sets the context to be used for connect.
func BadgerDataPath(path string) Option {
	return func(o *Options) error {
		o.BadgerDataPath = path
		return nil
	}
}

// BadgerWriteErr sets the callback to be triggered when there is an error
// writing a message to Badger.
func BadgerWriteMsgErr(cb func(*nats.Msg, error)) Option {
	return func(o *Options) error {
		o.BadgerWriteMsgErr = cb
		return nil
	}
}

// Options can be used to create a customized Service connections.
type Options struct {
	ctx context.Context

	// Nats
	NatsServers   string
	NatsSubject   string
	NatsQueueName string
	NatsOptions   []nats.Option
	NatsConnErrCB func(*Conn, error)

	// Badger
	BadgerDataPath    string
	BadgerWriteMsgErr func(*nats.Msg, error)
}

func GetDefaultOptions() Options {
	return Options{
		ctx:           context.Background(),
		NatsServers:   DefaultNatsServers,
		NatsSubject:   DefaultNatsSubject,
		NatsQueueName: DefaultNatsQueueName,
		NatsOptions: []nats.Option{
			nats.Name(DefaultNatsClientName),
			nats.RetryOnFailedConnect(DefaultNatsRetryOnFailure),
		},
	}
}

// Connect will attempt to connect to a NATS server with multiple options
// and setup connections to the disk database.
func (o Options) Connect() (*Conn, error) {
	rc := NewConn(o)

	if err := rc.initNATS(); err != nil {
		rc.Close()
		return nil, err
	}

	if err := rc.initBadger(); err != nil {
		rc.Close()
		return nil, err
	}

	// Start consumers to process messages
	if err := rc.initNatsConsumers(); err != nil {
		rc.Close()
		return nil, err
	}

	go func() {
		// Context closed.
		<-o.ctx.Done()
		rc.Close()
	}()

	// Setup the interrupt handler to drain so we don't miss
	// requests when scaling down.
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		os.Interrupt,
		syscall.SIGTERM, // AWS sometimes improperly uses SIGTERM.
	)

	go func() {
		<-c
		// Got interrupt. Close things down.
		rc.Close()
	}()

	return rc, nil
}

type closers struct {
	nats          *Closer
	natsConsumers *Closer
	badger        *Closer
}

type Conn struct {
	Opts Options

	mu sync.RWMutex

	// Nats
	nc        *nats.Conn
	sub       *nats.Subscription
	natsMsgCh chan *nats.Msg

	// Badger
	badgerDB *badger.DB

	closeOnce sync.Once
	closed    chan struct{}
	closers   closers
}

func NewConn(o Options) *Conn {
	return &Conn{
		Opts:      o,
		natsMsgCh: make(chan *nats.Msg),
		closed:    make(chan struct{}),
		closers: closers{
			nats:   NewCloser(0),
			badger: NewCloser(0),
		},
	}
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		log.Info().Msg("requeue: closing...")
		// Stop nats
		c.closers.nats.SignalAndWait()
		// Stop processing nats messages
		c.closers.natsConsumers.SignalAndWait()
		// Stop badger
		c.closers.badger.SignalAndWait()
		log.Info().Msg("requeue: closed")
		close(c.closed)
	})
}

func (c *Conn) HasBeenClosed() <-chan struct{} {
	return c.closed
}

func (c *Conn) NATSDisconnectErrHandler(nc *nats.Conn, err error) {
	log.Err(err).Msgf("nats: Got disconnected!")
}

func (c *Conn) NATSReconnectHandler(nc *nats.Conn) {
	// Note that this will be invoked for the first asynchronous connect.
	log.Info().Msgf("nats: Got reconnected to %s!", nc.ConnectedUrl())
}

func (c *Conn) NATSClosedHandler(nc *nats.Conn) {
	err := nc.LastError()
	log.Err(err).Msg("nats: Connection closed")
	if c.Opts.NatsConnErrCB != nil {
		c.Opts.NatsConnErrCB(c, err)
	}

	// Close anything left open (such as badger).
	c.Close()
}

func (c *Conn) initNATS() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	o := c.Opts
	rc := c

	// TODO(nickpoorman): We may want to provide our own callbacks for these
	// in case the user wants to hook into them as well.
	o.NatsOptions = append(o.NatsOptions,
		nats.DisconnectErrHandler(rc.NATSDisconnectErrHandler),
		nats.ReconnectHandler(rc.NATSReconnectHandler),
		nats.ClosedHandler(rc.NATSClosedHandler),
	)

	// Connect to NATS
	rc.nc, err = nats.Connect(o.NatsServers, o.NatsOptions...)
	if err != nil {
		log.Err(err).Msgf("NATS: unable to connec to servers: %s", o.NatsServers)
		// Because we retry our connection, this error would be a configuration error.
		return err
	}

	// Close nats when the closer is signaled.
	rc.closers.nats.AddRunning(1)
	go func() {
		defer rc.closers.nats.Done()
		<-c.closers.nats.HasBeenClosed()

		// Close nats
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.nc != nil {
			log.Debug().Msg("draining nats...")
			if err := c.nc.Drain(); err != nil {
				log.Err(err).Msg("error draining nats")
			}
			log.Debug().Msg("drained nats")

			log.Debug().Msg("closing nats...")
			c.nc.Close()
			log.Debug().Msg("closed nats")
		}
	}()

	// Subscribe to the subject using the queue group.
	sub, err := rc.nc.QueueSubscribeSyncWithChan(o.NatsSubject, o.NatsQueueName, c.natsMsgCh)
	if err != nil {
		log.Err(err).Dict("nats",
			zerolog.Dict().
				Str("subject", o.NatsSubject).
				Str("queue", o.NatsQueueName)).
			Msg("nats: unable to subscribe to queue")
		return err
	}
	rc.sub = sub
	rc.nc.Flush()

	if err := rc.nc.LastError(); err != nil {
		log.Err(err).Msg("nats: LastError")
		return err
	}

	log.Info().
		Dict("nats",
			zerolog.Dict().
				Str("subject", o.NatsSubject).
				Str("queue", o.NatsQueueName)).
		Msgf("Listening on [%s] in queue group [%s]", o.NatsSubject, o.NatsQueueName)

	return nil
}

func (c *Conn) initBadger() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	openOpts := badger.DefaultOptions(c.Opts.BadgerDataPath)
	openOpts.Logger = badgerLogger{}
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(openOpts)
	if err != nil {
		log.Err(err).Msgf("problem opening badger data path: %s", c.Opts.BadgerDataPath)
	}
	c.badgerDB = db

	c.closers.badger.AddRunning(1)
	go func() {
		defer c.closers.badger.Done()
		<-c.closers.badger.HasBeenClosed()
		// Badger cannot stop until nats has.
		// This probably isn't necessary since we already wait for it to close
		// before signaling badger to close, but adding it to be certain.
		<-c.closers.nats.HasBeenClosed()

		log.Debug().Msg("closing badger...")
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.badgerDB != nil {
			c.badgerDB.Close()
		}
		log.Debug().Msg("closed badger")
	}()

	return nil
}

func (c *Conn) initNatsConsumers() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	numConsumers := 1
	c.closers.natsConsumers.AddRunning(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go c.initNatsConsumer()
	}

	return nil
}

func (c *Conn) initNatsConsumer() {
	defer c.closers.natsConsumers.Done()

	wb := c.badgerDB.NewWriteBatch()
	// TODO: Ack the messages once they have been written. Flushed?
	for {
		select {
		case msg := <-c.natsMsgCh:
			fmt.Printf("Received a message: %s\n", string(msg.Data))
			// TODO: Get key and value from the message.
			key := []byte{}
			value := []byte{}
			if err := wb.Set(key, value); err != nil {
				log.Err(err).Msg("problem calling Set on WriteBatch")
				if c.Opts.BadgerWriteMsgErr != nil {
					c.Opts.BadgerWriteMsgErr(msg, err)
				}
			}
		case <-c.closers.natsConsumers.HasBeenClosed():
			// The consumer has been asked to close.
			// Flush our batch and then return.
			if err := wb.Flush(); err != nil {
				log.Err(err).Msg("problem calling Flush on WriteBatch")
				if c.Opts.BadgerWriteMsgErr != nil {
					c.Opts.BadgerWriteMsgErr(nil, err)
				}
			}
			return
		}
	}
}
