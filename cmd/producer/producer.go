package producer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"queue-lab/internal/pkg/common"
	"queue-lab/internal/pkg/dto"
	"queue-lab/internal/pkg/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	input     io.Reader
	chunkSize int64
}

func New(input io.Reader, chunkSize int64) Producer {
	return Producer{
		input:     input,
		chunkSize: chunkSize,
	}
}

func (p Producer) log(format string, values ...any) {
	utils.Log("[PRODUCER]", format, values...)
}

func (p Producer) Run(ctx context.Context, ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(common.ProducerExchange, "fanout", false, true, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	currentLines := []string{}
	var currentLength int64 = 0
	chunkID := 0

	reader := bufio.NewReader(p.input)

	sendChunk := func() error {
		p.log("Sending chunk %d", chunkID)

		msg := dto.ProducerMessage{
			ChunkID: chunkID,
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
		currentLength += int64(len(line))

		if currentLength >= p.chunkSize {
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

	p.log("Done processing file, sending fin")

	msg := dto.ProducerMessage{
		ChunkID: -1,
	}

	err := utils.Publish(ctx, ch, common.ProducerExchange, "", msg)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
