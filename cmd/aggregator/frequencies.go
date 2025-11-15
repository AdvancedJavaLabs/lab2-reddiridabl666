package aggregator

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (a Aggregator) aggregateFrequencies(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(common.FrequencyOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare input queue: %w", err)
	}

	frequencyQueue, err := ch.ConsumeWithContext(ctx, common.FrequencyOutput, "aggregator-frequencies"+id, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	wg.Go(func() {
		result := make(map[string]int)

		for message := range frequencyQueue {
			var msg dto.FrequencyResult

			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				a.log("Unmarshal error: %s", err)
				continue
			}

			if msg.Frequencies == nil {
				break
			}

			for word, count := range msg.Frequencies {
				result[word] += count
			}
		}

		sortedWordsByCount := slices.SortedFunc(maps.Keys(result), func(a, b string) int {
			return cmp.Compare(result[b], result[a])
		})

		sortedResultLength := min(a.topN, len(sortedWordsByCount))

		sortedResult := make([]dto.WordCount, 0, sortedResultLength)

		for _, word := range sortedWordsByCount[:sortedResultLength] {
			sortedResult = append(sortedResult, dto.WordCount{
				Word:  word,
				Count: result[word],
			})
		}

		err = utils.Publish(ctx, ch, "", common.AggregatorOutput, dto.AggregatorResult{
			Type: dto.ResultTypeTopN,
			TopN: sortedResult,
		})
		if err != nil {
			a.log("Publish error: %s", err)
		}
	})

	return nil
}
