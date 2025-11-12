package resultsink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ResultSink struct{}

func New() ResultSink {
	return ResultSink{}
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

	awaitingResults := 1

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
				result.Count = int(msg.Result.(float64))
			}

			resultTypes[dto.ResultTypeCount] = struct{}{}

			if len(resultTypes) >= awaitingResults {
				break
			}
		}
	})

	wg.Wait()

	file, err := os.Create("output.json")
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(result)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}

	r.log("Written result file")

	return nil
}
