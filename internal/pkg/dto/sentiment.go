package dto

type SentimentResult struct {
	Sentiment float64 `json:"sentiment"`
	Length    int     `json:"length"`
}
