package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Aggregator struct{}

func New() Aggregator {
	return Aggregator{}
}

func (a Aggregator) log(format string, values ...any) {
	utils.Log("[AGGREGATOR]", format, values...)
}

func (a Aggregator) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	counterQueue, err := ch.ConsumeWithContext(ctx, common.CounterOutput, "aggregator-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	_, err = ch.QueueDeclare(common.CounterOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.AggregatorOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		result := 0

		for message := range counterQueue {
			var msg dto.CounterResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				a.log("Unmarshal error: %s", err)
				continue
			}

			if msg.Count == -1 {
				break
			}

			result += msg.Count
		}

		a.log("Total count is: %d", result)

		err = utils.Publish(ctx, ch, "", common.AggregatorOutput, dto.AggregatorResult{
			Type:   dto.ResultTypeCount,
			Result: result,
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	wg.Wait()

	return nil
}
