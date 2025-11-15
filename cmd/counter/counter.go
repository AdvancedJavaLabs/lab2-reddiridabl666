package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Counter struct{}

func New() Counter {
	return Counter{}
}

func (c Counter) log(format string, values ...any) {
	utils.Log("[COUNTER]", format, values...)
}

func (c Counter) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.CounterInput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.CounterOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	err = ch.QueueBind(common.CounterInput, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, common.CounterInput, "counter-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		for message := range readChan {
			var msg dto.ProducerMessage

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				c.log("Unmarshal error: %s", err)
				continue
			}

			if msg.ChunkID == -1 {
				return
			}

			wg.Go(func() {
				normalized := common.Punctuation.ReplaceAllString(msg.Payload, " ")

				words := slices.DeleteFunc(strings.Split(normalized, " "), func(word string) bool {
					return word == ""
				})

				count := len(words)

				c.log("Word count of chunk %d is %d", msg.ChunkID, count)

				err := utils.Publish(ctx, ch, "", common.CounterOutput, dto.CounterResult{
					Count: count,
				})
				if err != nil {
					c.log("Publish error: %w", err)
				}
			})
		}
	})

	wg.Wait()

	c.log("Got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.CounterOutput, dto.CounterResult{
		Count: -1,
	})
	if err != nil {
		c.log("Publish error: %w", err)
	}

	return nil
}
