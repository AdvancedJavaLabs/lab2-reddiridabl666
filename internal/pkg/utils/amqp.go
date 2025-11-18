package utils

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Publish(ctx context.Context, ch *amqp.Channel, exchange, key string, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	err = ch.PublishWithContext(ctx, exchange, key, true, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        payload,
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
