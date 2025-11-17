package sorter

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/dto"
	"queue-lab/internal/pkg/utils"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sentencizer/sentencizer"
)

type Sorter struct {
	segmenter sentencizer.Segmenter
}

func New() Sorter {
	return Sorter{
		segmenter: sentencizer.NewSegmenter("en"),
	}
}

func (s Sorter) log(format string, values ...any) {
	utils.Log("[SORTER]", format, values...)
}

func (s Sorter) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.SorterInput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.SorterOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	err = ch.QueueBind(common.SorterInput, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, common.SorterInput, "sort-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		for message := range readChan {
			var msg dto.ProducerMessage

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				s.log("Unmarshal error: %s", err)
				continue
			}

			if msg.ChunkID == -1 {
				return
			}

			wg.Go(func() {
				sentences := s.segmenter.Segment(msg.Payload)

				sentences = slices.DeleteFunc(sentences, func(sentence string) bool {
					return len(sentence) < 2
				})

				slices.SortFunc(sentences, func(a, b string) int {
					return cmp.Compare(len(a), len(b))
				})

				err := utils.Publish(ctx, ch, "", common.SorterOutput, dto.SortResult{
					Sentences: sentences,
				})
				if err != nil {
					s.log("Publish error: %w", err)
				}
			})
		}
	})

	wg.Wait()

	s.log("Got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.SorterOutput, dto.SortResult{})
	if err != nil {
		s.log("Publish error: %w", err)
	}

	return nil
}
