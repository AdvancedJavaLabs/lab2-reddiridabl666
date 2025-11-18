package replacer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/dto"
	"queue-lab/internal/pkg/utils"

	"github.com/google/uuid"
	"github.com/jdkato/prose/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Replacer struct {
	replaceBy string
}

func New(replaceBy string) Replacer {
	return Replacer{
		replaceBy: replaceBy,
	}
}

func (r Replacer) log(format string, values ...any) {
	utils.Log("[REPLACER]", format, values...)
}

func (r Replacer) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.ReplacerInput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.ReplacerOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	err = ch.QueueBind(common.ReplacerInput, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, common.ReplacerInput, "replacer-"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg := sync.WaitGroup{}

	wg.Go(func() {
		for message := range readChan {
			var msg dto.ProducerMessage

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				r.log("Unmarshal error: %s", err)
				continue
			}

			if msg.ChunkID == -1 {
				return
			}

			wg.Go(func() {
				replacePatterns := []string{}

				doc, err := prose.NewDocument(msg.Payload,
					prose.WithSegmentation(false),
					prose.WithTagging(false))
				if err != nil {
					r.log("Error initializing prose document for chunk %d: %s", msg.ChunkID, err)
					return
				}

				for _, ent := range doc.Entities() {
					r.log("Replacing %s with %s", ent.Text, r.replaceBy)
					replacePatterns = append(replacePatterns, ent.Text, r.replaceBy)
				}

				replacer := strings.NewReplacer(replacePatterns...)

				payload := replacer.Replace(msg.Payload)

				err = utils.Publish(ctx, ch, "", common.ReplacerOutput, dto.ReplaceResult{
					ChunkID: msg.ChunkID,
					Payload: payload,
				})
				if err != nil {
					r.log("Publish error: %w", err)
				}
			})
		}
	})

	wg.Wait()

	r.log("Got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.ReplacerOutput, dto.ReplaceResult{
		ChunkID: -1,
	})
	if err != nil {
		r.log("Publish error: %w", err)
	}

	return nil
}
