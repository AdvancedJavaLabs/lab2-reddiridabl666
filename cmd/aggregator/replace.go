package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/dto"
	"queue-lab/internal/pkg/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (a Aggregator) aggregateReplaces(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(common.ReplacerOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	counterQueue, err := ch.ConsumeWithContext(ctx, common.ReplacerOutput, "aggregator-replaces"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg.Go(func() {
		result := strings.Builder{}

		lastChunk := 0

		chunks := map[int]string{}

		for message := range counterQueue {
			var msg dto.ReplaceResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				a.log("Unmarshal error: %s", err)
				continue
			}

			if msg.ChunkID == -1 {
				break
			}

			if msg.ChunkID > lastChunk {
				lastChunk = msg.ChunkID
			}

			chunks[msg.ChunkID] = msg.Payload
		}

		for i := range lastChunk + 1 {
			result.WriteString(chunks[i])
		}

		a.log("Aggregated replaced text")

		err = utils.Publish(ctx, ch, "", common.AggregatorOutput, dto.AggregatorResult{
			Type:     dto.ResultTypeReplace,
			Replaced: result.String(),
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	return nil
}
