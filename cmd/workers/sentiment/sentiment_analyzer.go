package sentiment

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/dto"
	"queue-lab/internal/pkg/utils"

	"github.com/jonreiter/govader"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SentimentAnalyzer struct {
	model *govader.SentimentIntensityAnalyzer
}

func New() SentimentAnalyzer {
	return SentimentAnalyzer{
		model: govader.NewSentimentIntensityAnalyzer(),
	}
}

func (s SentimentAnalyzer) log(format string, values ...any) {
	utils.Log("[SENTIMENT]", format, values...)
}

func (s SentimentAnalyzer) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.SentimentInput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	_, err = ch.QueueDeclare(common.SentimentOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	err = ch.QueueBind(common.SentimentInput, "", common.ProducerExchange, false, nil)
	if err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	readChan, err := ch.ConsumeWithContext(ctx, common.SentimentInput, "sentiment-"+id, true, false, false, false, nil)
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
				scores := s.model.PolarityScores(msg.Payload)

				s.log("Sentiment of chunk %d: %f", msg.ChunkID, scores.Compound)

				err := utils.Publish(ctx, ch, "", common.SentimentOutput, dto.SentimentResult{
					Sentiment: scores.Compound,
					Length:    len(msg.Payload),
				})
				if err != nil {
					s.log("Publish error: %w", err)
				}
			})
		}
	})

	wg.Wait()

	s.log("Got fin - exiting")

	err = utils.Publish(ctx, ch, "", common.SentimentOutput, dto.SentimentResult{})
	if err != nil {
		s.log("Publish error: %w", err)
	}

	return nil
}
