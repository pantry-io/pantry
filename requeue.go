package requeue

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

// ConnectContext sets the context to be used for connect.
func ConnectContext(ctx context.Context) Option {
	return func(o *Options) error {
		o.ctx = ctx
		return nil
	}
}

// Options can be used to create a customized Service connections.
type Options struct {
	ctx           context.Context
	NatsServers   string
	NatsSubject   string
	NatsQueueName string
	NatsOptions   []nats.Option
	NatsConnErrCB func(*Conn, error)
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
	ctx, cancel := context.WithCancel(o.ctx)
	o.ctx = ctx

	rc := &Conn{
		Opts:        o,
		stoppedNats: make(chan struct{}),
	}

	if err := rc.initNATS(); err != nil {
		cancel()
		return nil, err
	}

	// TODO: Connect to message database

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
		cancel()
	}()

	return rc, nil
}

type Conn struct {
	Opts Options

	// NATS
	nc  *nats.Conn
	sub *nats.Subscription

	stoppedNats chan struct{}
}

func (c *Conn) Stopped() <-chan struct{} {
	return c.stoppedNats
}

func (c *Conn) closeBadger() {
	// TODO: Close down badger (flush all writes to disk)
	// once NATS has been closed
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
}

func (c *Conn) initNATS() error {
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

	// Close nats when the context is cancelled.
	go func() {
		<-o.ctx.Done()

		// Close nats
		log.Info().Msg("draining nats...")
		if err := c.nc.Drain(); err != nil {
			log.Err(err).Msg("error draining nats")
		}
		log.Info().Msg("drained nats")

		log.Info().Msg("closing nats...")
		c.nc.Close()
		log.Info().Msg("closed nats")

		// TODO: Block until the ClosedHandler / DisconnectErrHandler are called?

		close(c.stoppedNats)
	}()

	// Subscribe to the subject using the queue group.
	sub, err := rc.nc.QueueSubscribe(o.NatsSubject, o.NatsQueueName, func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
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
