package dto

type Result struct {
	Count     int         `json:"count"`
	TopN      []WordCount `json:"topN"`
	Sentiment float64     `json:"sentiment"`
}
