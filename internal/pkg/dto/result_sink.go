package dto

type Result struct {
	Count     int         `json:"count"`
	TopN      []WordCount `json:"topN"`
	Sentiment float64     `json:"sentiment"`
	Sorted    []string    `json:"sorted"`
	Replaced  string      `json:"replaced"`
}
