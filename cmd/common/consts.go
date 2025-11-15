package common

import "regexp"

const ProducerExchange = "dws-parallel"

const (
	CounterInput  = "counter-input"
	CounterOutput = "counter-output"
)

const (
	FrequencyInput  = "frequency-input"
	FrequencyOutput = "frequency-output"
)

const (
	SentimentInput  = "sentiment-input"
	SentimentOutput = "sentiment-output"
)

const (
	ReplaceInput  = "replace-input"
	ReplaceOutput = "replace-output"
)

const (
	SortInput  = "sort-input"
	SortOutput = "sort-output"
)

const AggregatorOutput = "aggregator-output"

var Punctuation = regexp.MustCompile("[,.;:\"'()!?#\n]")
