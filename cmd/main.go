package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"queue-lab/cmd/counter"
	"queue-lab/cmd/producer"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler interface {
	Run(ctx context.Context, channel *amqp.Channel) error
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	waitCh := make(chan struct{})

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

	handlers := []Handler{
		producer.New(inputFile, func() { /* waitCh <- struct{}{} */ }),
		counter.New(),
	}

	for _, handler := range handlers {
		go func() {
			channel, err := connection.Channel()
			if err != nil {
				log.Fatal("Create channel:", err)
			}
			defer channel.Close()

			err = handler.Run(ctx, channel)
			if err != nil {
				log.Println("Handler error", err)
			}
		}()
	}

	<-waitCh
}
