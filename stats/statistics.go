package stats

// Statistics returns the total number of indexed documents along with a slice of the false negative
// errors for a variety of query thresholds. This can help determine if the configured number of
// hyperplanes and tables can give the desired results for a given threshold.
type Statistics struct {
	NumDocs             int                  `json:"num_docs"`
	FalseNegativeErrors []FalseNegativeError `json:"false_negative_errors"`
}

// FalseNegativeError represents the probability that a document will be missed during a search when it
// should be found. This document should match with the query document, but due to the number of
// hyperplanes, number of tables and the desired threshold will not with this probability. Closer to
// zero means there's less chance for missing document results and closer to 1 means a higher likelihood
// of missing the documents in the search.
type FalseNegativeError struct {
	Threshold   float64 `json:"threshold"`
	Probability float64 `json:"probability"`
}
