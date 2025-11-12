package dto

type ResultType string

const (
	ResultTypeCount ResultType = "count"
)

type AggregatorResult struct {
	Type   ResultType `json:"type"`
	Result any        `json:"result"`
}
