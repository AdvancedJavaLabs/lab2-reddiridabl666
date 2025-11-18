package aggregator

import (
	"context"
	"fmt"
	"sync"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/utils"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Aggregator struct {
	topN int
}

func New(topN int) Aggregator {
	return Aggregator{
		topN: topN,
	}
}

func (a Aggregator) log(format string, values ...any) {
	utils.Log("[AGGREGATOR]", format, values...)
}

func (a Aggregator) Run(ctx context.Context, ch *amqp.Channel) error {
	id := uuid.NewString()

	_, err := ch.QueueDeclare(common.AggregatorOutput, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare output queue: %w", err)
	}

	wg := sync.WaitGroup{}

	err = a.aggregateCounts(ctx, &wg, ch, id)
	if err != nil {
		return fmt.Errorf("aggregate counts: %w", err)
	}

	err = a.aggregateFrequencies(ctx, &wg, ch, id)
	if err != nil {
		return fmt.Errorf("aggregate frequencies: %w", err)
	}

	err = a.aggregateSentiment(ctx, &wg, ch, id)
	if err != nil {
		return fmt.Errorf("aggregate sentiment: %w", err)
	}

	err = a.aggregateSort(ctx, &wg, ch, id)
	if err != nil {
		return fmt.Errorf("aggregate sorted sentences: %w", err)
	}

	err = a.aggregateReplaces(ctx, &wg, ch, id)
	if err != nil {
		return fmt.Errorf("aggregate replaced text: %w", err)
	}

	wg.Wait()

	return nil
}
