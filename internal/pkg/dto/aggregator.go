package dto

type ResultType string

const (
	ResultTypeCount     ResultType = "count"
	ResultTypeTopN      ResultType = "topN"
	ResultTypeSentiment ResultType = "sentiment"
	ResultTypeReplace   ResultType = "replace"
	ResultTypeSort      ResultType = "sort"
)

type AggregatorResult struct {
	Type      ResultType  `json:"type"`
	Count     *int        `json:"count,omitempty"`
	TopN      []WordCount `json:"topN,omitempty"`
	Sentiment *float64    `json:"sentiment,omitempty"`
}

type WordCount struct {
	Word  string `json:"word"`
	Count int    `json:"count"`
}
