package resultsink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ResultSink struct {
	output io.Writer
}

func New(output io.Writer) ResultSink {
	return ResultSink{
		output: output,
	}
}

func (r ResultSink) log(format string, values ...any) {
	utils.Log("[RESULT SINK]", format, values...)
}

func (r ResultSink) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.AggregatorOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	aggregatorQueue, err := ch.ConsumeWithContext(ctx, common.AggregatorOutput, "resultsink-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	resultTypes := make(map[dto.ResultType]struct{})

	awaitingResults := 4

	result := dto.Result{}

	wg.Go(func() {
		for message := range aggregatorQueue {
			var msg dto.AggregatorResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				r.log("Unmarshal error: %s", err)
				continue
			}

			if _, ok := resultTypes[msg.Type]; ok {
				r.log("Already seen result for %s, skipping", msg.Type)
				continue
			}

			switch msg.Type {
			case dto.ResultTypeCount:
				result.Count = *msg.Count
				resultTypes[dto.ResultTypeCount] = struct{}{}

			case dto.ResultTypeTopN:
				result.TopN = msg.TopN
				resultTypes[dto.ResultTypeTopN] = struct{}{}

			case dto.ResultTypeSentiment:
				result.Sentiment = *msg.Sentiment
				resultTypes[dto.ResultTypeSentiment] = struct{}{}

			case dto.ResultTypeSort:
				result.Sorted = msg.Sorted
				resultTypes[dto.ResultTypeSort] = struct{}{}
			}

			if len(resultTypes) >= awaitingResults {
				break
			}
		}
	})

	wg.Wait()

	if r.output != nil {
		encoder := json.NewEncoder(r.output)

		err = encoder.Encode(result)
		if err != nil {
			return fmt.Errorf("encode result: %w", err)
		}

		r.log("Written result file")
	} else {
		fmt.Println("\nWord count:", result.Count)

		topN := ""
		for _, item := range result.TopN {
			topN += fmt.Sprintf("%s: %d; ", item.Word, item.Count)
		}

		fmt.Printf("Top %d words by frequency: %s\n", len(result.TopN), topN)

		fmt.Printf("Sentiment: %f\n", result.Sentiment)

		// fmt.Printf("Sorted: %v\n", result.Sorted)
	}

	return nil
}
