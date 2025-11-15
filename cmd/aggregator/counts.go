package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (a Aggregator) aggregateCounts(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(common.CounterOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	counterQueue, err := ch.ConsumeWithContext(ctx, common.CounterOutput, "aggregator-counts"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

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
			Type:  dto.ResultTypeCount,
			Count: &result,
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	return nil
}
