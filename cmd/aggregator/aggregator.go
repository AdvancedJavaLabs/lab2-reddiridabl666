package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Aggregator struct{}

func New() Aggregator {
	return Aggregator{}
}

func (p Aggregator) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	counterQueue, err := ch.ConsumeWithContext(ctx, common.CounterOutput, "aggregator-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		result := 0

		for message := range counterQueue {
			var msg dto.CounterResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				log.Println("unmarshal:", err)
				continue
			}

			if msg.Count == -1 {
				break
			}

			result += msg.Count
		}

		log.Println("[AGGREGATOR] Total count is:", result)
	})

	wg.Wait()

	return nil
}
