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
	fmt.Printf("Usage: requeue [-s server] [-creds file] [-sub subject] [-q queue]\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var urls = flag.String("s", requeue.DefaultNatsServers, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var subj = flag.String("sub", requeue.DefaultNatsSubject, "The subject to subscribe to for messages")
	var queueName = flag.String("q", requeue.DefaultNatsQueueName, "Queue Group Name")
	var clientName = flag.String("client-name", requeue.DefaultNatsClientName, "The NATS client name")
	var showHelp = flag.Bool("h", false, "Show help message")

	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	// NATS connect Options.
	natsOpts := requeue.GetDefaultOptions().NatsOptions

	natsOpts = append(natsOpts, []nats.Option{
		nats.Name(*clientName),
		nats.MaxReconnects(4),
	}...)

	// NATS user UserCredentials
	if *userCreds != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*userCreds))
	}

	ctx := context.Background()

	rc, err := requeue.Connect(
		requeue.ConnectContext(ctx),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(*urls),
		requeue.NATSSubject(*subj),
		requeue.NATSQueueName(*queueName),
	)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to connec to NATS server")
	}
	<-rc.Stopped()
	log.Info().Msg("requeue terminated.")
}
