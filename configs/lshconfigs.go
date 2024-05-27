package configs

import (
	"errors"
	"fmt"

	"gonum.org/v1/gonum/floats"
)

const (
	// key value is expected to be at most 16 bits
	maxNumHyperplanes = 16
)

var (
	ErrExceededMaxNumHyperplanes = fmt.Errorf("number of hyperplanes exceeded max of, %d", maxNumHyperplanes)
	ErrInvalidNumHyperplanes     = errors.New("invalid number of hyperplanes, must be at least 1")
	ErrInvalidNumTables          = errors.New("invalid number of tables, must be at least 1")
	ErrInvalidVectorLength       = errors.New("invalid vector length, must be at least 1")
	ErrInvalidSamplePeriod       = errors.New("invalid sample period, must be at least 1")
	ErrInvalidRowSize            = errors.New("invalid row size, must be at least 1")
)

type TransformFunc func([]float64) []float64

func NewDefaultTransformFunc(vec []float64) []float64 {
	floats.Scale(1.0/floats.Norm(vec, 2), vec)
	return vec
}

// LSHConfigs represents a set of parameters that configure the LSH tables
type LSHConfigs struct {
	NumHyperplanes int
	NumTables      int
	VectorLength   int
	SamplePeriod   int64         // expected time period between each sample in the vector
	RowSize        int64         // size of each range of store bitmaps per table. Larger values will generally store more uids
	TFunc          TransformFunc // transformation to vector on index and search
}

// NewDefaultLSHConfigs returns a set of default options to create the LSH tables
func NewDefaultLSHConfigs() *LSHConfigs {
	return &LSHConfigs{
		NumHyperplanes: 8,   // more hyperplanes increases false negatives decrease number of direct comparisons
		NumTables:      128, // more tables means we'll decrease false negatives at the cost of more direct comparisons
		VectorLength:   3,
		SamplePeriod:   60,   // defaults to 1m between each sample in the vector
		RowSize:        7200, // if the index represents seconds from epoch then this would translate to a table window of 2hrs
		TFunc:          NewDefaultTransformFunc,
	}
}

// Validate returns an error if any of the LSH options are invalid
func (c *LSHConfigs) Validate() error {
	if c.NumHyperplanes < 1 {
		return ErrInvalidNumHyperplanes
	}
	if c.NumHyperplanes > maxNumHyperplanes {
		return ErrExceededMaxNumHyperplanes
	}

	if c.NumTables < 1 {
		return ErrInvalidNumTables
	}

	if c.VectorLength < 1 {
		return ErrInvalidVectorLength
	}

	if c.SamplePeriod < 1 {
		return ErrInvalidSamplePeriod
	}

	if c.RowSize < 1 {
		return ErrInvalidRowSize
	}

	return nil
}
