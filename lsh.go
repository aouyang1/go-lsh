package lsh

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/RoaringBitmap/roaring/roaring64"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

const (
	// key value is expected to be at most 64 bits
	MaxNumHyperplanes = 64

	// default label to use if none specified
	DefaultLabel = "__DEFAULT__"

	// default value to use if none specified
	DefaultValue = "__DEFAULT__"
)

var (
	ErrExceededMaxNumHyperplanes = fmt.Errorf("number of hyperplanes exceeded max of, %d", MaxNumHyperplanes)
	ErrInvalidNumHyperplanes     = errors.New("invalid number of hyperplanes, must be at least 1")
	ErrInvalidNumTables          = errors.New("invalid number of tables, must be at least 1")
	ErrInvalidNumFeatures        = errors.New("invalid number of features, must be at least 1")
	ErrInvalidDocument           = errors.New("number of features does not match with the configured options")
	ErrDuplicateDocument         = errors.New("document is already indexed")
	ErrNoOptions                 = errors.New("no options set for LSH")
	ErrNoFeatureComplexity       = errors.New("features do not have enough complexity with a standard deviation of 0")
	ErrFeatureLengthMismatch     = errors.New("feature slice length mismatch")
	ErrNoFeatures                = errors.New("no features provided")
	ErrDocumentNotStored         = errors.New("document id is not stored")
	ErrHashNotFound              = errors.New("hash not found in table")
	ErrInvalidNumToReturn        = errors.New("invalid NumToReturn, must be at least 1")
	ErrInvalidThreshold          = errors.New("invalid threshold, must be between 0 and 1 inclusive")
	ErrInvalidSignFilter         = errors.New("invalid sign filter, must be any, neg, or pos")
)

// Options represents a set of parameters that configure the LSH tables
type Options struct {
	NumHyperplanes int
	NumTables      int
	NumFeatures    int
}

// NewDefaultOptions returns a set of default options to create the LSH tables
func NewDefaultOptions() *Options {
	return &Options{
		NumHyperplanes: 8,   // more hyperplanes increases false negatives decrease number of direct comparisons
		NumTables:      128, // more tables means we'll decrease false negatives at the cost of more direct comparisons
		NumFeatures:    3,
	}
}

// Validate returns an error if any of the LSH options are invalid
func (o *Options) Validate() error {
	if o.NumHyperplanes < 1 {
		return ErrInvalidNumHyperplanes
	}
	if o.NumHyperplanes > MaxNumHyperplanes {
		return ErrExceededMaxNumHyperplanes
	}

	if o.NumTables < 1 {
		return ErrInvalidNumTables
	}

	if o.NumFeatures < 1 {
		return ErrInvalidNumFeatures
	}

	return nil
}

// LSH represents the locality sensitive hash struct that stores the multiple tables containing
// the configured number of hyperplanes along with the documents currently indexed.
type LSH struct {
	Opt              *Options
	HyperplaneTables []*Hyperplanes
	Tables           map[string]map[string][]*Table // label to value to a set of tables
	Docs             map[uint64]Document
}

// New returns a new Locality Sensitive Hash struct ready for indexing and searching
func New(opt *Options) (*LSH, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.Opt = opt

	l.Tables = make(map[string]map[string][]*Table)

	l.HyperplaneTables = make([]*Hyperplanes, 0, opt.NumTables)
	for i := 0; i < opt.NumTables; i++ {
		ht, err := NewHyperplanes(opt.NumHyperplanes, opt.NumFeatures)
		if err != nil {
			return nil, err
		}
		l.HyperplaneTables = append(l.HyperplaneTables, ht)
	}
	l.Docs = make(map[uint64]Document)
	return l, nil
}

// Index stores the document in the LSH data structure. Returns an error if the document
// is already present. If byLabels is nil the default label used is an empty string which would
// be the default label/value keyspace documents are indexed. Setting byLabels permits
// more scoped queries while searching e.g. find similar features with label = foo, value = bar.
func (l *LSH) Index(d Document, byLabels []string) error {
	uid := d.GetUID()
	feat := d.GetFeatures()
	if len(feat) != l.Opt.NumFeatures {
		return ErrInvalidDocument
	}
	if stat.StdDev(feat, nil) == 0 {
		return ErrNoFeatureComplexity
	}
	if _, exists := l.Docs[uid]; exists {
		return ErrDuplicateDocument
	}
	floats.Scale(1.0/floats.Norm(feat, 2), feat)

	if byLabels == nil {
		byLabels = []string{DefaultLabel}
	}
	for _, label := range byLabels {
		if err := l.index(d, label); err != nil {
			return err
		}
	}
	l.Docs[uid] = d
	return nil
}

func (l *LSH) index(d Document, label string) error {
	var err error

	v, exists := d.GetLabel(label)
	if !exists {
		v = DefaultValue
	}
	values, exists := l.Tables[label]
	if !exists {
		values = make(map[string][]*Table)
		l.Tables[label] = values
	}

	tables, exists := values[v]
	if !exists {
		tables, err = NewTables(l.Opt, l.HyperplaneTables)
		if err != nil {
			return err
		}
		values[v] = tables
	}
	for _, t := range tables {
		if err := t.index(d); err != nil {
			return err
		}
	}
	return nil
}

// Delete attempts to remove the uid from the tables and also the document map
func (l *LSH) Delete(uid uint64) error {
	var err error
	for _, values := range l.Tables {
		for _, tables := range values {
			for _, t := range tables {
				if e := t.delete(uid); e != nil {
					err = e
				}
			}
		}
	}
	delete(l.Docs, uid)
	return err
}

// SearchOptions represent a set of parameters to be used to customize search results
type SearchOptions struct {
	Query       map[string][]string `json:"query"`
	NumToReturn int                 `json:"num_to_return"`
	Threshold   float64             `json:"threshold"`
	SignFilter  SignFilter          `json:"sign_filter"`
}

// Validate returns an error if any of the input options are invalid
func (s *SearchOptions) Validate() error {
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
	return nil
}

// NewDefaultSearchOptions returns a default set of parameters to be used for search. If query
// is not set then the search will be performed across all label value tables.
func NewDefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		NumToReturn: 10,
		Threshold:   0.85,
		SignFilter:  SignFilter_ANY,
	}
}

// Search looks through and merges results from all tables to find the nearest neighbors to the
// provided feature vector
func (l *LSH) Search(f []float64, s *SearchOptions) (Scores, int, error) {
	if len(f) != l.Opt.NumFeatures {
		return nil, 0, ErrInvalidDocument
	}

	if err := s.Validate(); err != nil {
		return nil, 0, err
	}
	floats.Scale(1.0/floats.Norm(f, 2), f)

	res := NewResults(s.NumToReturn, s.Threshold, SignFilter_POS)

	// search for positively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_POS {
		if err := l.search(s.Query, f, res); err != nil {
			return nil, 0, err
		}
	}

	// search for negatively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_NEG {
		floats.Scale(-1, f)
		if err := l.search(s.Query, f, res); err != nil {
			return nil, 0, err
		}
	}

	return res.Fetch(), res.NumScored, nil
}

func (l *LSH) search(query map[string][]string, f []float64, res *Results) error {
	rbRes := roaring64.New()

	// search across all label value tables
	if query == nil {
		query = make(map[string][]string)
		for label, valTables := range l.Tables {
			query[label] = make([]string, 0, len(valTables))
			for value := range valTables {
				query[label] = append(query[label], value)
			}
		}
	}

	// if label is specified but values is of length 0 then search across all values with
	// that label
	for label, values := range query {
		if len(values) > 0 {
			continue
		}
		valTables, exists := l.Tables[label]
		if !exists {
			continue
		}

		query[label] = make([]string, 0, len(valTables))
		for val := range valTables {
			query[label] = append(query[label], val)
		}
	}

	// search through each label and value tables
	for label, values := range query {
		valTables, exists := l.Tables[label]
		if !exists {
			continue
		}
		for _, v := range values {
			tables, exists := valTables[v]
			if !exists {
				continue
			}
			for _, t := range tables {
				hash, err := t.Hyperplanes.Hash64(f)
				if err != nil {
					return err
				}
				rb := t.Table[hash]
				if rb == nil {
					// feature vector hash not present in hyperplane partition
					continue
				}
				rb.mu.Lock()
				rbRes.Or(rb.Rb)
				rb.mu.Unlock()
			}
		}
	}

	var numScored int
	for _, uid := range rbRes.ToArray() {
		doc, exists := l.Docs[uid]
		if !exists || doc == nil {
			continue
		}
		score := stat.Correlation(f, doc.GetFeatures(), nil)
		res.Update(Score{uid, score})
		numScored++
	}

	return nil
}

// Save takes a filepath and a document interface representing the indexed documents
// and saves the lsh index to disk. Only one type of document is currently supported
// which will be registered with gob to encode and save to disk.
func (l *LSH) Save(filepath string, d Document) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	d.Register()

	if err := enc.Encode(l); err != nil {
		return err
	}
	return nil
}

func (l *LSH) Load(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)

	var lsh LSH
	if err := dec.Decode(&lsh); err != nil {
		return err
	}

	*l = lsh
	return nil
}

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
// zero means there's less chance for missing document results and closer to 1 means a higher liklihood
// of missing the documents in the search.
type FalseNegativeError struct {
	Threshold   float64 `json:"threshold"`
	Probability float64 `json:"probability"`
}

// Stats returns the current statistics about the configured LSH struct.
func (l *LSH) Stats() *Statistics {
	s := new(Statistics)
	s.NumDocs = len(l.Docs)

	thetaInc := 0.05
	thetaStart := 0.60
	thetaEnd := 1.0

	// compute false negative errors for various thresholds
	s.FalseNegativeErrors = make([]FalseNegativeError, 0, int((thetaEnd-thetaStart)/thetaInc))
	for theta := thetaStart; theta < thetaEnd; theta += thetaInc {
		pdiff := 2 / math.Pi * math.Acos(theta)
		psame := 1 - pdiff

		fneg := math.Pow((1 - math.Pow(psame, float64(l.Opt.NumHyperplanes))), float64(l.Opt.NumTables))

		s.FalseNegativeErrors = append(s.FalseNegativeErrors, FalseNegativeError{theta, fneg})
	}
	return s
}
