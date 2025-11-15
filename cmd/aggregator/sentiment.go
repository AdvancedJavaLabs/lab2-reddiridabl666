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

func (a Aggregator) aggregateSentiment(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(common.SentimentOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	sentimentQueue, err := ch.ConsumeWithContext(ctx, common.SentimentOutput, "aggregator-sentiment"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg.Go(func() {
		var result float64 = 0
		length := 0

		for message := range sentimentQueue {
			var msg dto.SentimentResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				a.log("Unmarshal error: %s", err)
				continue
			}

			if msg.Length == 0 {
				break
			}

			length += msg.Length

			result += msg.Sentiment * float64(msg.Length)
		}

		result /= float64(length)

		a.log("Total sentiment: %f", result)

		err = utils.Publish(ctx, ch, "", common.AggregatorOutput, dto.AggregatorResult{
			Type:      dto.ResultTypeSentiment,
			Sentiment: &result,
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	return nil
}
