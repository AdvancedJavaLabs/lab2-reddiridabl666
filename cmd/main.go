package main

import (
	"context"
	"flag"
	"log"
	"os"
	"sync"

	"queue-lab/cmd/aggregator"
	"queue-lab/cmd/counter"
	frequency "queue-lab/cmd/frequency_counter"
	"queue-lab/cmd/producer"
	resultsink "queue-lab/cmd/result_sink"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler interface {
	Run(ctx context.Context, channel *amqp.Channel) error
}

func main() {
	// ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	// defer cancel()

	ctx := context.Background()

	inputFilePath := flag.String("in", "assets/sample.txt", "Input file path")

	flag.Parse()

	connection, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal("Connect to RabbitMQ:", err)
	}
	defer connection.Close()

	inputFile, err := os.Open(*inputFilePath)
	if err != nil {
		log.Fatal("Open input file:", err)
	}
	defer inputFile.Close()

	wg := sync.WaitGroup{}

	handlers := []Handler{
		producer.New(inputFile),
		counter.New(),
		frequency.New(),
		aggregator.New(),
		resultsink.New(),
	}

	for _, handler := range handlers {
		wg.Go(func() {
			channel, err := connection.Channel()
			if err != nil {
				log.Fatal("Create channel:", err)
			}
			defer channel.Close()

			err = handler.Run(ctx, channel)
			if err != nil {
				log.Println("Handler error", err)
			}
		})
	}

	wg.Wait()
}
