package dto

type MessageType string

const (
	MessageTypeTask MessageType = "task"
	MessageTypeFin  MessageType = "fin"
)

type ProducerMessage struct {
	ID      int         `json:"id"`
	Type    MessageType `json:"type"`
	Payload string      `json:"payload"`
}
