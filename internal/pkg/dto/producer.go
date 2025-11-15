package dto

type MessageType string

type ProducerMessage struct {
	ChunkID int    `json:"chunkID"`
	Payload string `json:"payload"`
}
