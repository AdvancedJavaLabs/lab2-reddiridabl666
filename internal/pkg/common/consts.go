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
	ReplacerInput  = "replace-input"
	ReplacerOutput = "replace-output"
)

const (
	SorterInput  = "sort-input"
	SorterOutput = "sort-output"
)

const AggregatorOutput = "aggregator-output"

var Punctuation = regexp.MustCompile("[,.;:\"'()!?#\n]")
