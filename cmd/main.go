package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"sync"

	"queue-lab/cmd/aggregator"
	"queue-lab/cmd/counter"
	"queue-lab/cmd/frequency"
	"queue-lab/cmd/producer"
	resultsink "queue-lab/cmd/result_sink"
	"queue-lab/cmd/sentiment"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler interface {
	Run(ctx context.Context, channel *amqp.Channel) error
}

var (
	inputFilePath = flag.String("in", "assets/sample.txt", "Input file path")

	outputFilePath = flag.String("o", "", "Output file path")

	topN = flag.Int("n", 5, "Number of top word frequencies")

	minLength = flag.Int("minLength", 1, "Minimum word length for the word to be included in the top N output")
)

func main() {
	ctx := context.Background()

	flag.Parse()

	connection, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal("Connect to RabbitMQ:", err)
	}
	defer connection.Close()

	if *topN < 1 {
		*topN = 1
	}

	inputFile, err := os.Open(*inputFilePath)
	if err != nil {
		log.Fatal("Open input file:", err)
	}
	defer inputFile.Close()

	var outputFile io.WriteCloser

	if *outputFilePath != "" {
		outputFile, err = os.Create(*outputFilePath)
		if err != nil {
			log.Fatal("Open output file:", err)
		}
		defer outputFile.Close()
	}

	wg := sync.WaitGroup{}

	handlers := []Handler{
		producer.New(inputFile),
		counter.New(),
		frequency.New(*minLength),
		sentiment.New(),
		aggregator.New(*topN),
		resultsink.New(outputFile),
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
