package lsh

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

const (
	// key value is expected to be at most 64 bits
	MaxNumHyperplanes = 64
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
		NumHyperplanes: 32,
		NumTables:      3,
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
	Opt    *Options
	Tables []*Table
	Docs   map[uint64]*Document
}

// New returns a new Locality Sensitive Hash struct ready for indexing and searching
func New(opt *Options) (*LSH, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.Opt = opt

	var err error

	l.Tables = make([]*Table, l.Opt.NumTables)
	for i := 0; i < l.Opt.NumTables; i++ {
		l.Tables[i], err = NewTable(l.Opt.NumHyperplanes, l.Opt.NumFeatures)
		if err != nil {
			return nil, err
		}
	}

	// TODO: instead of storing the original document, we should store the document with the
	// uid along with values that are needed to compute the pearson correlation between 2 samples.
	// This means storage of the docs scale linearly by number of documents and not by number of
	// documents AND number of features per document. Computation time per correlation should be
	// reduced as well.
	l.Docs = make(map[uint64]*Document)
	return l, nil
}

// Index stores the document in the LSH data structure. Returns an error if the document
// is already present and will attempt to rollback any changes to other tables.
func (l *LSH) Index(d *Document) error {
	if len(d.Features) != l.Opt.NumFeatures {
		return ErrInvalidDocument
	}
	if stat.StdDev(d.Features, nil) == 0 {
		return ErrNoFeatureComplexity
	}
	if _, exists := l.Docs[d.UID]; exists {
		return ErrDuplicateDocument
	}
	floats.Scale(1.0/floats.Norm(d.Features, 2), d.Features)

	for i, t := range l.Tables {
		if err := t.index(d); err != nil {
			// attempt to roll back removing added document to other tables
			var derr error
			for j := 0; j < i; j++ {
				if e := l.Tables[j].delete(d.UID); e != nil {
					derr = e
				}
			}
			return fmt.Errorf("%v, %v", err, derr)
		}
	}
	l.Docs[d.UID] = d
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

// NewDefaultSearchOptions returns a default set of parameters to be used for search
func NewDefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		NumToReturn: 10,
		Threshold:   0.85,
		SignFilter:  SignFilter_ANY,
	}
}

// Search looks through and merges results from all tables to find the nearest neighbors to the
// provided feature vector
func (l *LSH) Search(f []float64, s *SearchOptions) (Scores, error) {
	if len(f) != l.Opt.NumFeatures {
		return nil, ErrInvalidDocument
	}

	if err := s.Validate(); err != nil {
		return nil, err
	}
	floats.Scale(1.0/floats.Norm(f, 2), f)

	res := NewResults(s.NumToReturn, s.Threshold, SignFilter_POS)

	// search for positively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_POS {
		if err := l.search(f, res); err != nil {
			return nil, err
		}
	}

	// search for negatively correlated results
	if s.SignFilter == SignFilter_ANY || s.SignFilter == SignFilter_NEG {
		floats.Scale(-1, f)
		if err := l.search(f, res); err != nil {
			return nil, err
		}
	}

	return res.Fetch(), nil
}

func (l *LSH) search(f []float64, res *Results) error {
	rbRes := roaring64.New()
	for _, t := range l.Tables {
		hash, err := t.Hyperplanes.hash(f)
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

	for _, uid := range rbRes.ToArray() {
		doc, exists := l.Docs[uid]
		if !exists || doc == nil {
			continue
		}
		score := stat.Correlation(f, doc.Features, nil)
		res.Update(Score{uid, score})
	}
	return nil
}

func (l *LSH) Save(filepath string) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)

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

type Table struct {
	Hyperplanes *Hyperplanes
	Table       map[uint64]*Bitmap
	Doc2Hash    map[uint64]uint64
}

func NewTable(numHyper, numFeat int) (*Table, error) {
	t := new(Table)

	var err error
	t.Hyperplanes, err = NewHyperplanes(numHyper, numFeat)
	if err != nil {
		return nil, err
	}

	t.Table = make(map[uint64]*Bitmap)
	t.Doc2Hash = make(map[uint64]uint64)
	return t, nil
}

func (t *Table) index(d *Document) error {
	if _, exists := t.Doc2Hash[d.UID]; exists {
		return ErrDuplicateDocument
	}

	hash, err := t.Hyperplanes.hash(d.Features)
	if err != nil {
		return err
	}
	rb, exists := t.Table[hash]
	if !exists || rb == nil {
		rb = newBitmap()
		t.Table[hash] = rb
	}

	if !rb.CheckedAdd(d.UID) {
		return fmt.Errorf("unable to add %d to bitmap at hash, %d", d.UID, hash)
	}

	t.Doc2Hash[d.UID] = hash
	return nil
}

func (t *Table) delete(uid uint64) error {
	hash, exists := t.Doc2Hash[uid]
	if !exists {
		return ErrDocumentNotStored
	}

	rb, exists := t.Table[hash]
	if !exists {
		return ErrHashNotFound
	}

	if !rb.CheckedRemove(uid) {
		return fmt.Errorf("unable to remove %d from bitmap at hash, %d", uid, hash)
	}

	if rb.IsEmpty() {
		delete(t.Table, hash)
	}
	delete(t.Doc2Hash, uid)
	return nil
}

type Bitmap struct {
	mu sync.Mutex
	Rb *roaring64.Bitmap
}

func newBitmap() *Bitmap {
	return &Bitmap{Rb: roaring64.New()}
}

func (b *Bitmap) CheckedAdd(uid uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.CheckedAdd(uid)
}

func (b *Bitmap) CheckedRemove(uid uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.CheckedRemove(uid)
}

func (b *Bitmap) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.IsEmpty()
}

type Document struct {
	UID      uint64
	Features []float64
}

func NewDocument(uid uint64, f []float64) *Document {
	return &Document{
		UID:      uid,
		Features: f,
	}
}
