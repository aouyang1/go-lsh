package options

import (
	"errors"
)

var (
	ErrInvalidNumToReturn = errors.New("invalid NumToReturn, must be at least 1")
	ErrInvalidThreshold   = errors.New("invalid threshold, must be between 0 and 1 inclusive")
	ErrInvalidSignFilter  = errors.New("invalid sign filter, must be any, neg, or pos")
)

const (
	AllLags = -1 // indicates we want all matches regardless of index position
)

type SignFilter int

const (
	SignFilter_POS = 1
	SignFilter_NEG = -1
	SignFilter_ANY = 0
)

// SearchOptions represent a set of parameters to be used to customize search results
type Search struct {
	NumToReturn int        `json:"num_to_return"`
	Threshold   float64    `json:"threshold"`
	SignFilter  SignFilter `json:"sign_filter"`
	MaxLag      int64      `json:"max_lag"` // -1 means any lag
}

// Validate returns an error if any of the input options are invalid
func (s *Search) Validate() error {
	if s.NumToReturn < 1 {
		return ErrInvalidNumToReturn
	}
	if s.Threshold < 0 || s.Threshold > 1 {
		return ErrInvalidThreshold
	}
	switch s.SignFilter {
	case SignFilter_ANY, SignFilter_NEG, SignFilter_POS:
	default:
		return ErrInvalidSignFilter
	}

	if s.MaxLag < AllLags {
		s.MaxLag = AllLags
	}

	return nil
}

// NewDefaultSearch returns a default set of parameters to be used for search.
func NewDefaultSearch() *Search {
	return &Search{
		NumToReturn: 10,
		Threshold:   0.85,
		SignFilter:  SignFilter_ANY,
		MaxLag:      900, // translates to 15m if index is seconds from epoch
	}
}
