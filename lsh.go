package lsh

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
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
	ErrInvalidDocument           = errors.New("vector length does not match with the configured options")
	ErrDuplicateDocument         = errors.New("document is already indexed")
	ErrNoOptions                 = errors.New("no options set for LSH")
	ErrNoVectorComplexity        = errors.New("vector does not have enough complexity with a standard deviation of 0")
	ErrVectorLengthMismatch      = errors.New("vector length mismatch")
	ErrNoVector                  = errors.New("no vector provided")
	ErrDocumentNotStored         = errors.New("document id is not stored")
	ErrHashNotFound              = errors.New("hash not found in table")
	ErrInvalidNumToReturn        = errors.New("invalid NumToReturn, must be at least 1")
	ErrInvalidThreshold          = errors.New("invalid threshold, must be between 0 and 1 inclusive")
	ErrInvalidSignFilter         = errors.New("invalid sign filter, must be any, neg, or pos")
)

type TransformFunc func([]float64) []float64

func NewDefaultTransformFunc(vec []float64) []float64 {
	floats.Scale(1.0/floats.Norm(vec, 2), vec)
	return vec
}

// Options represents a set of parameters that configure the LSH tables
type Options struct {
	NumHyperplanes int
	NumTables      int
	VectorLength   int
	SamplePeriod   int64         // expected time period between each sample in the vector
	RowSize        int64         // size of each range of store bitmaps per table. Larger values will generally store more uids
	tFunc          TransformFunc // transformation to vector on index and search
}

// NewDefaultOptions returns a set of default options to create the LSH tables
func NewDefaultOptions() *Options {
	return &Options{
		NumHyperplanes: 8,   // more hyperplanes increases false negatives decrease number of direct comparisons
		NumTables:      128, // more tables means we'll decrease false negatives at the cost of more direct comparisons
		VectorLength:   3,
		SamplePeriod:   60,   // defaults to 1m between each sample in the vector
		RowSize:        7200, // if the index represents seconds from epoch then this would translate to a table window of 2hrs
		tFunc:          NewDefaultTransformFunc,
	}
}

// Validate returns an error if any of the LSH options are invalid
func (o *Options) Validate() error {
	if o.NumHyperplanes < 1 {
		return ErrInvalidNumHyperplanes
	}
	if o.NumHyperplanes > maxNumHyperplanes {
		return ErrExceededMaxNumHyperplanes
	}

	if o.NumTables < 1 {
		return ErrInvalidNumTables
	}

	if o.VectorLength < 1 {
		return ErrInvalidVectorLength
	}

	if o.SamplePeriod < 1 {
		return ErrInvalidSamplePeriod
	}

	if o.RowSize < 1 {
		return ErrInvalidRowSize
	}

	return nil
}

// LSH represents the locality sensitive hash struct that stores the multiple tables containing
// the configured number of hyperplanes along with the documents currently indexed.
type LSH struct {
	Opt    *Options
	Tables []*Table            // N tables each using a different randomly generated set of hyperplanes
	Docs   map[uint64]Document // forward index which may be offloaded to a separate system
}

// New returns a new Locality Sensitive Hash struct ready for indexing and searching
func New(opt *Options) (*LSH, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.Opt = opt

	hyperplaneTables := make([]*Hyperplanes, 0, opt.NumTables)
	for i := 0; i < opt.NumTables; i++ {
		ht, err := NewHyperplanes(opt.NumHyperplanes, opt.VectorLength)
		if err != nil {
			return nil, err
		}
		hyperplaneTables = append(hyperplaneTables, ht)
	}
	tables, err := NewTables(l.Opt, hyperplaneTables)
	if err != nil {
		return nil, err
	}
	l.Tables = tables

	l.Docs = make(map[uint64]Document)
	return l, nil
}

// Index stores the document in the LSH data structure. Returns an error if the document
// is already present.
func (l *LSH) Index(d Document) error {
	origDoc := d.Copy()
	uid := d.GetUID()
	vec := d.GetVector()
	if len(vec) != l.Opt.VectorLength {
		return ErrInvalidDocument
	}
	if stat.StdDev(vec, nil) == 0 {
		return ErrNoVectorComplexity
	}
	if _, exists := l.Docs[uid]; exists {
		return ErrDuplicateDocument
	}

	vec = l.Opt.tFunc(vec)

	if err := l.index(d); err != nil {
		return err
	}

	// expand current doc of the uid if present
	if currDoc, exists := l.Docs[origDoc.GetUID()]; exists {
		dIdx := origDoc.GetIndex() / l.Opt.SamplePeriod
		cdIdx := currDoc.GetIndex() / l.Opt.SamplePeriod
		offset := int(dIdx - cdIdx)

		origVec := origDoc.GetVector()
		cdVec := currDoc.GetVector()
		if offset > 0 {
			for i := 0; i < len(origVec); i++ {
				idx := i + offset
				if idx < len(cdVec) {
					cdVec[idx] = origVec[i]
				} else {
					zeroPad := idx - len(cdVec)
					if zeroPad > 0 {
						zeros := make([]float64, zeroPad)
						cdVec = append(cdVec, zeros...)
					}
					cdVec = append(cdVec, origVec[i])
				}
			}
		} else {
			// not handling docs that are in the past
		}
	} else {
		l.Docs[uid] = d
	}
	return nil
}

func (l *LSH) index(d Document) error {
	for _, t := range l.Tables {
		if err := t.index(d); err != nil {
			return err
		}
	}
	return nil
}

// Delete attempts to remove the uid from the tables and also the document map
func (l *LSH) Delete(uid uint64) error {
	var err error
	for _, t := range l.Tables {
		if e := t.delete(uid); e != nil {
			err = e
		}
	}
	delete(l.Docs, uid)
	return err
}

// SearchOptions represent a set of parameters to be used to customize search results
type SearchOptions struct {
	NumToReturn int        `json:"num_to_return"`
	Threshold   float64    `json:"threshold"`
	SignFilter  SignFilter `json:"sign_filter"`
	MaxLag      int64      `json:"max_lag"` // -1 means any lag
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

	if s.MaxLag < -1 {
		s.MaxLag = -1
	}

	return nil
}

// NewDefaultSearchOptions returns a default set of parameters to be used for search.
func NewDefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		NumToReturn: 10,
		Threshold:   0.85,
		SignFilter:  SignFilter_ANY,
		MaxLag:      900, // translates to 15m if index is seconds from epoch
	}
}

// Search looks through and merges results from all tables to find the nearest neighbors to the
// provided vector
func (l *LSH) Search(d Document, s *SearchOptions) (Scores, int, error) {
	v := d.GetVector()
	if len(v) != l.Opt.VectorLength {
		return nil, 0, ErrInvalidDocument
	}

	if s == nil {
		s = NewDefaultSearchOptions()
	} else {
		if err := s.Validate(); err != nil {
			return nil, 0, err
		}
	}

	docIds, err := l.Filter(d, s)
	if err != nil {
		return nil, 0, err
	}
	res := NewResults(s.NumToReturn, s.Threshold, SignFilter_POS)
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_POS {
		l.Score(d, docIds, s.MaxLag, res)
	}

	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_NEG {
		floats.Scale(-1, d.GetVector())
		l.Score(d, docIds, s.MaxLag, res)
		floats.Scale(-1, d.GetVector())
	}
	return res.Fetch(), res.NumScored, nil
}

// Filter returns a set of document ids that match the given vector and search options
func (l *LSH) Filter(d Document, s *SearchOptions) ([]uint64, error) {
	vec := d.GetVector()
	if len(vec) != l.Opt.VectorLength {
		return nil, ErrInvalidDocument
	}

	if s == nil {
		s = NewDefaultSearchOptions()
	} else {
		if err := s.Validate(); err != nil {
			return nil, err
		}
	}

	l.Opt.tFunc(vec)

	var docIds []uint64
	// search for positively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_POS {
		dids, err := l.filter(d, s.MaxLag)
		if err != nil {
			return nil, err
		}
		docIds = append(docIds, dids...)
	}

	// search for negatively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_NEG {
		floats.Scale(-1, vec)
		dids, err := l.filter(d, s.MaxLag)
		if err != nil {
			floats.Scale(-1, vec) // undo negation
			return nil, err
		}
		floats.Scale(-1, vec) // undo negation
		docIds = append(docIds, dids...)
	}

	return docIds, nil
}

func (l *LSH) filter(d Document, maxLag int64) ([]uint64, error) {
	rbRes := roaring64.New()
	var resLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(l.Tables))

	for _, t := range l.Tables {
		go func(tbl *Table) {
			defer wg.Done()
			rowRbRes := tbl.filter(d, maxLag)

			resLock.Lock()
			rbRes.Or(rowRbRes)
			resLock.Unlock()
		}(t)
	}
	wg.Wait()

	return rbRes.ToArray(), nil
}

// Score takes a set of document ids and scores them against a provided search query
func (l *LSH) Score(d Document, docIds []uint64, maxLag int64, res *Results) {
	buffer := make([]float64, l.Opt.VectorLength)
	for _, uid := range docIds {
		doc, exists := l.Docs[uid]
		if !exists || doc == nil {
			continue
		}
		idx := d.GetIndex()
		vec := doc.GetVector()
		dIdx := doc.GetIndex()

		// just does 0 lag
		startOffset := int((idx - dIdx) / l.Opt.SamplePeriod)
		endOffset := startOffset + l.Opt.VectorLength
		if endOffset > len(vec) {
			endOffset = len(vec)
		}

		for i := 0; i < len(buffer); i++ {
			buffer[i] = 0.0
		}
		copy(buffer, vec[startOffset:endOffset])
		score := stat.Correlation(d.GetVector(), buffer, nil)
		res.Update(Score{uid, score})
	}
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
// zero means there's less chance for missing document results and closer to 1 means a higher likelihood
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
