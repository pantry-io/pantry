package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/rs/zerolog/log"
)

func usage() {
	fmt.Printf("Usage: requeue -d <data-dir> [-s server] [-creds file] [-sub subject] [-nats-queue-group group]\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

var showHelp = flag.Bool("h", false, "Show help message")
var dataDir = flag.String("d", "", "The directory where data for instances will be stored")
var natsListenSubject = flag.String("nats-listen-subject", requeue.DefaultNatsSubject, "The subject to listen for messages on. These are the messages that will be queued up.")
var natsURLs = flag.String("nats-urls", requeue.DefaultNatsServers, "Comma separated NATS server URLs ")
var natsClientName = flag.String("nats-client-name", requeue.DefaultNatsClientName, "NATS client name")
var natsQueueGroup = flag.String("nats-queue-group", requeue.DefaultNatsQueueName, "Queue Group Name")
var natsUserCredsFile = flag.String("nats-creds", "", "User Credentials File")

func main() {
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	if *dataDir == "" {
		showUsageAndExit(0)
	}

	// NATS connect Options.
	natsOpts := []nats.Option{
		nats.Name(*natsClientName),
		nats.MaxReconnects(10),
	}

	// NATS user UserCredentials.
	if *natsUserCredsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*natsUserCredsFile))
	}

	ctx := context.Background()

	rc, err := requeue.Connect(
		requeue.ConnectContext(ctx),
		requeue.DataDir(*dataDir),
		requeue.BadgerWriteMsgErr(badgerWriteMsgErr),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(*natsURLs),
		requeue.NATSSubject(*natsListenSubject),
		requeue.NATSQueueName(*natsQueueGroup),
	)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("problem with requeue connect")
	}
	<-rc.HasBeenClosed()
	log.Info().Msg("requeue: terminated.")
}

func badgerWriteMsgErr(msg *nats.Msg, err error) {
	log.Err(err).Interface("msg", msg.Data).Msg("problem writing message to Badger")
	// This would be a good place to add extra logic such as optimistically
	// replying to the message indicating there was a problem. i.e. Non-Ack
}
