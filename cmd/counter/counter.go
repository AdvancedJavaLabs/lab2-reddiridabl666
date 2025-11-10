package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"queue-lab/cmd/common"
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

func (p Counter) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.New()

	_, err := ch.QueueDeclare(inputQueue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	err = ch.QueueBind(inputQueue, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, inputQueue, "counter-"+id.String(), true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		for message := range readChan {
			var task dto.Task

			err = json.Unmarshal(message.Body, &task)
			if err != nil {
				log.Println("unmarshal:", err)
				continue
			}

			go func() {
				normalized := punctuation.ReplaceAllString(task.Payload, " ")
				count := len(strings.Split(normalized, " "))

				fmt.Printf("Word count of chunk %d is %d\n", task.ID, count)
			}()
		}
	}()

	<-ctx.Done()

	return nil
}
