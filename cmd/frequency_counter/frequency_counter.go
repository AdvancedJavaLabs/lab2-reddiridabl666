package frequency

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FrequencyCounter struct {
	minUnicodeLength int
}

func New(minUnicodeLength int) FrequencyCounter {
	return FrequencyCounter{
		minUnicodeLength: minUnicodeLength,
	}
}

func (c FrequencyCounter) log(format string, values ...any) {
	utils.Log("[FREQUENCY]", format, values...)
}

func (f FrequencyCounter) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.FrequencyInput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.FrequencyOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	err = ch.QueueBind(common.FrequencyInput, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, common.FrequencyInput, "frequency-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		for message := range readChan {
			var msg dto.ProducerMessage

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				f.log("Unmarshal error: %s", err)
				continue
			}

			if msg.Type == dto.MessageTypeFin {
				return
			}

			wg.Go(func() {
				frequencies := make(map[string]int)

				normalized := common.Punctuation.ReplaceAllString(msg.Payload, " ")

				for word := range strings.SplitSeq(normalized, " ") {
					if utf8.RuneCountInString(word) >= f.minUnicodeLength {
						frequencies[strings.ToLower(word)] += 1
					}
				}

				// f.log("Word frequencies of chunk %d is %d", msg.ID, frequencies)

				err := utils.Publish(ctx, ch, "", common.FrequencyOutput, dto.FrequencyResult{
					Frequencies: frequencies,
				})
				if err != nil {
					f.log("Publish error: %w", err)
				}
			})
		}
	})

	wg.Wait()

	f.log("Got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.FrequencyOutput, dto.FrequencyResult{
		Frequencies: nil,
	})
	if err != nil {
		f.log("Publish error: %w", err)
	}

	return nil
}
