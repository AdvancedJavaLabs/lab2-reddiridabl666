package dto

type CounterResult struct {
	Count int `json:"count"`
}

type FrequencyResult struct {
	Frequencies map[string]int `json:"frequencies"`
}

type SentimentResult struct {
	Sentiment float64 `json:"sentiment"`
	Length    int     `json:"length"`
}

type SortResult struct {
	Sentences []string `json:"sentences"`
}

type ReplaceResult struct {
	ChunkID int    `json:"chunkID"`
	Payload string `json:"payload"`
}
