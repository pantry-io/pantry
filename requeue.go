package requeue

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/nickpoorman/nats-requeue/internal/queue"
	"github.com/nickpoorman/nats-requeue/internal/reaper"
	"github.com/nickpoorman/nats-requeue/internal/republisher"
	"github.com/nickpoorman/nats-requeue/protocol"
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

	keySeperator byte = '.'

	DefaultNumConcurrentBatchTransactions = 4
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
		o.natsServers = natsServers
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
		o.natsSubject = natsSubject
		return nil
	}
}

// NatsQueueName is the queue to subscribe to. Messages from the queue will be
// distributed amongst the the subscribers of the queue.
func NATSQueueName(natsQueueName string) Option {
	return func(o *Options) error {
		o.natsQueueName = natsQueueName
		return nil
	}
}

// NATSOptions are options that will be provided to NATS upon establishing a
// connection.
func NATSOptions(natsOptions []nats.Option) Option {
	return func(o *Options) error {
		o.natsOptions = append(o.natsOptions, natsOptions...)
		return nil
	}
}

// NATSConnectionError is a callback when the connection is unable to be
// established.
func NATSConnectionError(connErrCb func(*Conn, error)) Option {
	return func(o *Options) error {
		o.natsConnErrCB = connErrCb
		return nil
	}
}

// DataDir is the directory where data will be stored. An instance of the data
// store will be created in this directory. This directory is also looped over
// by the reaper looking for zombie instances that need to be merged into the
// main data instance created at initialization.
func DataDir(path string) Option {
	return func(o *Options) error {
		o.dataDir = path
		return nil
	}
}

// BadgerWriteMsgErr sets the callback to be triggered when there is an error
// writing a message to Badger.
func BadgerWriteMsgErr(cb func(*nats.Msg, error)) Option {
	return func(o *Options) error {
		o.badgerWriteMsgErr = cb
		return nil
	}
}

// RepublisherOpts sets the options for the republisher.
func RepublisherOptions(options ...republisher.Option) Option {
	return func(o *Options) error {
		o.republisherOpts = append(o.republisherOpts, options...)
		return nil
	}
}

// TODO: These options should probably be lower case so they are private.
// Options can be used to create a customized Service connections.
type Options struct {
	ctx context.Context

	// Nats
	natsServers   string
	natsSubject   string
	natsQueueName string
	natsOptions   []nats.Option
	natsConnErrCB func(*Conn, error)

	// Badger
	dataDir           string
	badgerWriteMsgErr func(*nats.Msg, error)

	// Republisher
	republisherOpts []republisher.Option
}

func GetDefaultOptions() Options {
	return Options{
		ctx:           context.Background(),
		natsServers:   DefaultNatsServers,
		natsSubject:   DefaultNatsSubject,
		natsQueueName: DefaultNatsQueueName,
		natsOptions: []nats.Option{
			nats.Name(DefaultNatsClientName),
			nats.RetryOnFailedConnect(DefaultNatsRetryOnFailure),
		},
	}
}

// Connect will attempt to connect to a NATS server with multiple options
// and setup connections to the disk database.
func (o Options) Connect() (*Conn, error) {
	rc := NewConn(o)

	if err := rc.initBadger(); err != nil {
		rc.Close()
		return nil, err
	}

	if err := rc.initNATS(); err != nil {
		rc.Close()
		return nil, err
	}

	// Start consumers to process messages.
	if err := rc.initNatsConsumers(); err != nil {
		rc.Close()
		return nil, err
	}

	// Start up the service responsible for requeuing messages.
	if err := rc.initNatsProducers(); err != nil {
		rc.Close()
		return nil, err
	}

	// Start up the zombie badger store reaper.
	if err := rc.initReaper(); err != nil {
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
	nats          *y.Closer
	natsConsumers *y.Closer
	badger        *y.Closer
	reaper        *y.Closer
	natsProducers *y.Closer
}

type Conn struct {
	Opts Options

	mu sync.RWMutex

	// Nats
	nc        *nats.Conn
	sub       *nats.Subscription
	natsMsgCh chan *nats.Msg

	// Badger
	badgerDB    *badger.DB
	instanceId  string
	instanceDir string

	// Badger Reaper
	reaper *reaper.Reaper

	// Queues
	qManager    *queue.Manager
	republisher *republisher.Republisher

	closeOnce sync.Once
	closed    chan struct{}
	closers   closers
}

func NewConn(o Options) *Conn {
	instanceId := uuid.Must(uuid.NewV4()).String()
	return &Conn{
		Opts:        o,
		natsMsgCh:   make(chan *nats.Msg),
		closed:      make(chan struct{}),
		instanceId:  instanceId,
		instanceDir: filepath.Join(o.dataDir, instanceId),
		closers: closers{
			nats:          y.NewCloser(0),
			natsConsumers: y.NewCloser(0),
			badger:        y.NewCloser(0),
			reaper:        y.NewCloser(0),
			natsProducers: y.NewCloser(0),
		},
	}
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		log.Info().Msg("requeue: closing...")
		// Stop the nats producers from sending out messages on nats.
		c.closers.natsProducers.SignalAndWait()
		// Stop nats
		c.closers.nats.SignalAndWait()
		// Stop processing nats messages
		c.closers.natsConsumers.SignalAndWait()
		// Stop the reaper
		c.closers.reaper.SignalAndWait()
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
	log.Err(err).Msgf("nats-replay: Got disconnected!")
}

func (c *Conn) NATSErrorHandler(con *nats.Conn, sub *nats.Subscription, natsErr error) {
	log.Err(natsErr).Msgf("nats-replay: Got err: conn=%s sub=%s err=%v!", con.Opts.Name, sub.Subject, natsErr)

	if natsErr == nats.ErrSlowConsumer {
		pendingMsgs, _, err := sub.Pending()
		if err != nil {
			log.Err(err).Msg("nats-replay: couldn't get pending messages")
			return
		}
		log.Err(err).Msgf("nats-replay: Falling behind with %d pending messages on subject %q.\n",
			pendingMsgs, sub.Subject)
		// Log error, notify operations...
	}
	// check for other errors
}

func (c *Conn) NATSReconnectHandler(nc *nats.Conn) {
	// Note that this will be invoked for the first asynchronous connect.
	log.Info().Msgf("nats-replay: Got reconnected to %s!", nc.ConnectedUrl())
}

func (c *Conn) NATSClosedHandler(nc *nats.Conn) {
	err := nc.LastError()
	log.Err(err).Msg("nats-replay: Connection closed")
	if c.Opts.natsConnErrCB != nil {
		c.Opts.natsConnErrCB(c, err)
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
	o.natsOptions = append(o.natsOptions,
		nats.DisconnectErrHandler(rc.NATSDisconnectErrHandler),
		nats.ReconnectHandler(rc.NATSReconnectHandler),
		nats.ClosedHandler(rc.NATSClosedHandler),
		nats.ErrorHandler(rc.NATSErrorHandler),
	)

	// Connect to NATS
	rc.nc, err = nats.Connect(o.natsServers, o.natsOptions...)
	if err != nil {
		log.Err(err).Msgf("nats-replay: unable to connec to servers: %s", o.natsServers)
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

	sub, err := rc.nc.QueueSubscribe(o.natsSubject, o.natsQueueName, func(msg *nats.Msg) {
		// fb := flatbuf.GetRootAsRequeueMessage(msg.Data, 0)
		// log.Debug().Str("msg", string(fb.OriginalPayloadBytes())).Msg("got message")
		c.natsMsgCh <- msg
		// log.Debug().Str("msg", string(fb.OriginalPayloadBytes())).Msg("processed message")
	})

	// Subscribe to the subject using the queue group.
	// sub, err := rc.nc.QueueSubscribeSyncWithChan(o.NatsSubject, o.NatsQueueName, c.natsMsgCh)
	if err != nil {
		log.Err(err).Dict("nats",
			zerolog.Dict().
				Str("subject", o.natsSubject).
				Str("queue", o.natsQueueName)).
			Msg("nats-replay: unable to subscribe to queue")
		return err
	}
	// if err := sub.SetPendingLimits(10000, -1); err != nil {
	// 	log.Err(err).Msg("nats-replay: SetPendingLimits")
	// 	// Don't die, we'll just continue with the default limits.
	// }

	rc.sub = sub
	rc.nc.Flush()

	if err := rc.nc.LastError(); err != nil {
		log.Err(err).Msg("nats-replay: LastError")
		return err
	}

	log.Info().
		Dict("nats",
			zerolog.Dict().
				Str("subject", o.natsSubject).
				Str("queue", o.natsQueueName)).
		Msgf("Listening on [%s] in queue group [%s]", o.natsSubject, o.natsQueueName)

	return nil
}

func (c *Conn) initBadger() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a new instance in our dataDir
	if err := os.MkdirAll(c.instanceDir, os.ModePerm); err != nil {
		return fmt.Errorf("init badger: create instance directory: %w", err)
	}

	// We will then create a new instance in this dir.
	db, err := badgerInternal.Open(c.instanceDir)
	if err != nil {
		log.Err(err).Msgf("problem opening badger data path: %s", c.Opts.dataDir)
		return err
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.closers.natsConsumers.AddRunning(DefaultNumConcurrentBatchTransactions)

	for i := 0; i < DefaultNumConcurrentBatchTransactions; i++ {
		go c.initNatsConsumer()
	}

	return nil
}

func (c *Conn) initNatsConsumer() {
	c.mu.RLock()
	natsConsumer := c.closers.natsConsumers
	defer natsConsumer.Done()

	wb := badgerInternal.NewBatchedWriter(c.badgerDB, 15*time.Millisecond)
	defer wb.Close()
	c.mu.RUnlock()

	for {
		select {
		case msg := <-c.natsMsgCh:
			c.processIngressMessage(wb, msg)
		case <-natsConsumer.HasBeenClosed():
			// The consumer has been asked to close.
			// Flushing will be handled by the above defer wb.Close()
			return
		}
	}
}

func (c *Conn) processIngressMessage(wb *badgerInternal.BatchedWriter, msg *nats.Msg) {
	fb := flatbuf.GetRootAsRequeueMessage(msg.Data, 0)
	// decoded := RequeueMessageFromNATS(msg)
	log.Debug().
		Str("msg", string(fb.OriginalPayloadBytes())).
		Msg("received a message")

	// Build the key
	qk, err := c.newMessageQueueKey(msg, fb)
	if err != nil {
		return
	}

	// Before we write the message, we need to create the state for the
	// queue if it doesn't yet exist.
	stateQK := queue.NewQueueKeyForState(qk.Name, "")
	if err := c.qManager.UpsertQueueState(stateQK); err != nil {
		log.Err(err).
			Interface("stateQueueKey", stateQK).
			Msg("problem upserting queue state for ingress message")
	}

	if err := wb.SetEntry(
		badger.NewEntry(qk.Bytes(), msg.Data).WithTTL(time.Duration(fb.Ttl())),
		c.processIngressMessageCallback(msg)); err != nil {
		log.Err(err).Msg("problem calling SetEntry on WriteBatch")
		if c.Opts.badgerWriteMsgErr != nil {
			c.Opts.badgerWriteMsgErr(msg, err)
		}
	}
}

func (c *Conn) newMessageQueueKey(msg *nats.Msg, fb *flatbuf.RequeueMessage) (queue.QueueKey, error) {
	return queue.NewQueueKeyForMessage(
		protocol.GetQueueName(fb),
		key.New(time.Now().Add(time.Duration(fb.Delay()))),
	), nil
}

// A commit from batchedWriter will trigger a batch of callbacks,
// one for each message.
func (c *Conn) processIngressMessageCallback(msg *nats.Msg) func(err error) {
	return func(err error) {
		fb := flatbuf.GetRootAsRequeueMessage(msg.Data, 0)
		if err != nil {
			log.Err(err).
				Str("msg", string(fb.OriginalPayloadBytes())).
				Msgf("problem committing message")
		}
		// ml, bl, err := c.sub.PendingLimits()
		// if err != nil {
		// 	log.Err(err).Msg("PendingLimits")
		// }
		log.Debug().
			Str("msg", string(fb.OriginalPayloadBytes())).
			// Int("pending-limits-msg", ml).
			// Int("pending-limits-size", bl).
			Str("Reply", msg.Reply).
			Str("Subject", msg.Subject).
			Msgf("committed message")

		// Ack the message
		if err := msg.Respond(nil); err != nil {
			log.Err(err).
				Str("msg", string(fb.OriginalPayloadBytes())).
				Msgf("problem sending ACK for message")
		}
	}
}

func (c *Conn) initNatsProducers() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Load up all the queues we have on disk and manage them.
	manager, err := queue.NewManager(c.badgerDB)
	if err != nil {
		return err
	}
	c.qManager = manager

	// Create a republisher
	c.republisher, err = republisher.New(c.nc, c.badgerDB, manager, c.Opts.republisherOpts...)
	if err != nil {
		return err
	}

	c.closers.natsProducers.AddRunning(1)
	go func() {
		defer c.closers.natsProducers.Done()
		<-c.closers.natsProducers.HasBeenClosed()

		log.Debug().Msg("closing nats producers...")
		c.mu.Lock()
		defer c.mu.Unlock()

		// close the republisher
		if c.republisher != nil {
			c.republisher.Close()
		}

		// close the queue manager
		if c.qManager != nil {
			c.qManager.Close()
		}
	}()

	return nil
}

func (c *Conn) initReaper() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create our reaper
	reaper, err := reaper.NewReaper(c.Opts.dataDir, c.instanceDir)
	if err != nil {
		return err
	}
	c.reaper = reaper

	c.closers.reaper.AddRunning(1)
	go func() {
		defer c.closers.reaper.Done()
		<-c.closers.reaper.HasBeenClosed()

		log.Debug().Msg("closing reaper...")
		c.mu.Lock()
		defer c.mu.Unlock()

		// close the reaper
		if c.reaper != nil {
			c.reaper.Close()
		}
	}()

	return nil
}
