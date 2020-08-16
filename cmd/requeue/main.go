package main

// import (
// 	"context"
// 	"flag"
// 	"fmt"
// 	"os"

// 	"github.com/nats-io/nats.go"
// 	requeue "github.com/nickpoorman/nats-requeue"
// 	"github.com/rs/zerolog/log"
// )

// func usage() {
// 	fmt.Printf("Usage: requeue [-s server] [-creds file] [-sub subject] [-q queue]\n")
// 	flag.PrintDefaults()
// }

// func showUsageAndExit(exitcode int) {
// 	usage()
// 	os.Exit(exitcode)
// }

// func main() {
// 	var urls = flag.String("s", requeue.DefaultNatsServers, "The nats server URLs (separated by comma)")
// 	var userCreds = flag.String("creds", "", "User Credentials File")
// 	var subj = flag.String("sub", requeue.DefaultNatsSubject, "The subject to subscribe to for messages")
// 	var queueName = flag.String("q", requeue.DefaultNatsQueueName, "Queue Group Name")
// 	var clientName = flag.String("client-name", requeue.DefaultNatsClientName, "The NATS client name")
// 	var showHelp = flag.Bool("h", false, "Show help message")

// 	flag.Usage = usage
// 	flag.Parse()

// 	if *showHelp {
// 		showUsageAndExit(0)
// 	}

// 	// NATS connect Options.
// 	natsOpts := requeue.GetDefaultOptions().NatsOptions
// 	natsOpts = append(natsOpts, []nats.Option{
// 		nats.Name(*clientName),
// 		nats.MaxReconnects(4),
// 	}...)

// 	// NATS user UserCredentials
// 	if *userCreds != "" {
// 		natsOpts = append(natsOpts, nats.UserCredentials(*userCreds))
// 	}

// 	// Badger directory. We generate one automatically.
// 	badgerDataDir := "/tmp/badger"
// 	badgerDataStableId := "123" // ksuid.New().String()

// 	ctx := context.Background()

// 	rc, err := requeue.Connect(
// 		requeue.ConnectContext(ctx),
// 		requeue.BadgerDataPath(badgerDataPath(badgerDataDir, badgerDataStableId)),
// 		requeue.BadgerWriteMsgErr(badgerWriteMsgErr),
// 		requeue.NATSOptions(natsOpts),
// 		requeue.NATSServers(*urls),
// 		requeue.NATSSubject(*subj),
// 		requeue.NATSQueueName(*queueName),
// 	)
// 	if err != nil {
// 		log.Fatal().
// 			Err(err).
// 			Msg("unable to connec to NATS server")
// 	}
// 	<-rc.HasBeenClosed()
// 	log.Info().Msg("requeue: terminated.")
// }

// func badgerDataPath(path string, stableId string) string {
// 	return fmt.Sprintf("%s/%s", path, stableId)
// }

// func badgerWriteMsgErr(msg *nats.Msg, err error) {
// 	log.Err(err).Interface("msg", msg.Data).Msg("problem writing message to Badger")
// 	// This would be a good place to add extra logic such as optimistically
// 	// replying to the message indicating there was a problem. i.e. Non-Ack
// }
