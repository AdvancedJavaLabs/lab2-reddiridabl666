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

func (a Aggregator) aggregateSort(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(common.SortOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	counterQueue, err := ch.ConsumeWithContext(ctx, common.SortOutput, "aggregator-sort"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg.Go(func() {
		var result []string

		for message := range counterQueue {
			var msg dto.SortResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				a.log("Unmarshal error: %s", err)
				continue
			}

			if msg.Sentences == nil {
				break
			}

			result = utils.Merge(result, msg.Sentences, func(a, b string) bool {
				return len(a) < len(b)
			})
		}

		a.log("Merged sorted sentences")

		err = utils.Publish(ctx, ch, "", common.AggregatorOutput, dto.AggregatorResult{
			Type:   dto.ResultTypeSort,
			Sorted: result,
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	return nil
}
