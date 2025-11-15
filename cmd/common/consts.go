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

const AggregatorOutput = "aggregator-output"

var Punctuation = regexp.MustCompile("[,..;:\"'()!?#\n]")
