package producer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"queue-lab/cmd/common"
	"queue-lab/cmd/utils"
	"queue-lab/internal/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	chunkSize = 256
)

type Producer struct {
	input io.Reader
}

func New(input io.Reader) Producer {
	return Producer{
		input: input,
	}
}

func (p Producer) Run(ctx context.Context, ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(common.ProducerExchange, "fanout", false, true, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	currentLines := []string{}
	currentLength := 0
	chunkID := 0

	reader := bufio.NewReader(p.input)

	sendChunk := func() error {
		log.Println("Sending chunk", chunkID)

		msg := dto.ProducerMessage{
			ID:      chunkID,
			Type:    dto.MessageTypeTask,
			Payload: strings.Join(currentLines, "\n"),
		}

		err := utils.Publish(ctx, ch, common.ProducerExchange, "", msg)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		currentLength = 0
		chunkID++
		currentLines = nil

		return nil
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return fmt.Errorf("read from input file: %w", err)
			}
		}

		currentLines = append(currentLines, line)
		currentLength += len(line)

		if currentLength >= chunkSize {
			err = sendChunk()
			if err != nil {
				return fmt.Errorf("send chunk: %w", err)
			}
		}
	}

	if currentLength > 0 {
		err := sendChunk()
		if err != nil {
			return fmt.Errorf("send chunk: %w", err)
		}
	}

	log.Println("Done processing file, sending fin")

	msg := dto.ProducerMessage{
		Type: dto.MessageTypeFin,
	}

	err := utils.Publish(ctx, ch, common.ProducerExchange, "", msg)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
