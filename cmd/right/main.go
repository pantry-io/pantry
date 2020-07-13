package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
)

const Name = "consumertest"

func cleanUp(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		fmt.Println(err)
	}
}

func setup() string {
	dir, err := ioutil.TempDir("", fmt.Sprintf("%s-*", Name))
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	return dir
}

func main() {
	// s := natsserver.RunDefaultServer()
	// defaultOpts := natsserver.DefaultTestOptions
	// defaultOpts.NoSigs = false
	// s := natsserver.RunServer(&defaultOpts)
	// t.Cleanup(func() {
	// 	s.Shutdown()
	// })

	dir := setup()
	defer cleanUp(dir)
	dataPath := fmt.Sprintf("%s/%s_%s", dir, Name, ksuid.New().String())
	subject := "requeue.msgs"

	// clientURL := s.ClientURL()
	clientURL := "localhost:4222"
	log.Info().Msgf("running nats on: %s", clientURL)

	// NATS connect Options.
	natsOpts := requeue.GetDefaultOptions().NatsOptions
	natsOpts = append(natsOpts, []nats.Option{
		nats.Name(fmt.Sprintf("test_requeue_%s", Name)),
		nats.MaxReconnects(10),
	}...)

	rc, err := requeue.Connect(
		requeue.ConnectContext(context.Background()),
		requeue.BadgerDataPath(dataPath),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(clientURL),
		requeue.NATSSubject(subject),
		requeue.NATSQueueName(requeue.DefaultNatsQueueName),
	)
	if err != nil {
		log.Fatal().Msgf("Error on requeue connect: %v", err)
	}

	// TODO: Verify the events were written to disk

	// TODO:
	// 1. Shut down the requeue instance
	// 2. Then start it back up
	// 3. Spin up a consumer on the original subject
	// 4. Verify we get the requeued messages.

	<-rc.HasBeenClosed()

	log.Info().Msg("right terminated")
}
