package dto

type Result struct {
	Count int         `json:"count"`
	TopN  []WordCount `json:"topN"`
}
