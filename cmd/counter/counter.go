package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	inputQueue  = "counter-input"
	outputQueue = "counter-output"
)

var punctuation = regexp.MustCompile("[,..;:\"'()!?#]\n")

type Counter struct{}

func New() Counter {
	return Counter{}
}

func (с Counter) Run(ctx context.Context, ch *amqp.Channel) error {
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
				log.Println("unmarshal:", err)
				continue
			}

			if msg.Type == dto.MessageTypeFin {
				return
			}

			wg.Go(func() {
				normalized := punctuation.ReplaceAllString(msg.Payload, " ")
				count := len(strings.Split(normalized, " "))

				log.Printf("Word count of chunk %d is %d\n", msg.ID, count)

				err := utils.Publish(ctx, ch, "", common.CounterOutput, dto.CounterResult{
					Count: count,
				})
				if err != nil {
					log.Println("publish:", err)
				}
			})
		}
	})

	wg.Wait()

	log.Println("Counter got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.CounterOutput, dto.CounterResult{
		Count: -1,
	})
	if err != nil {
		log.Println("publish:", err)
	}

	return nil
}
