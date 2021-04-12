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
	errExceededMaxNumHyperplanes = fmt.Errorf("number of hyperplanes exceeded max of, %d", MaxNumHyperplanes)
	errInvalidNumHyperplanes     = errors.New("invalid number of hyperplanes, must be at least 1")
	errInvalidNumTables          = errors.New("invalid number of tables, must be at least 1")
	errInvalidNumFeatures        = errors.New("invalid number of features, must be at least 1")
	errInvalidDocument           = errors.New("number of features does not match with the configured options")
	errNoOptions                 = errors.New("no options set for LSH")
	errFeatureLengthMismatch     = errors.New("feature slice length mismatch")
	errNoFeatures                = errors.New("no features provided")
	errDocumentExists            = errors.New("document already exists")
	errDocumentNotStored         = errors.New("document id is not stored")
	errHashNotFound              = errors.New("hash not found in table")
)

type Options struct {
	NumHyperplanes int
	NumTables      int
	NumFeatures    int
}

func NewDefaultOptions() *Options {
	return &Options{
		NumHyperplanes: 32,
		NumTables:      3,
		NumFeatures:    3,
	}
}

func (o *Options) Validate() error {
	if o.NumHyperplanes < 1 {
		return errInvalidNumHyperplanes
	}
	if o.NumHyperplanes > MaxNumHyperplanes {
		return errExceededMaxNumHyperplanes
	}

	if o.NumTables < 1 {
		return errInvalidNumTables
	}

	if o.NumFeatures < 1 {
		return errInvalidNumFeatures
	}

	return nil
}

type LSH struct {
	Opt    *Options
	Tables []*Table
	Docs   map[uint64]*Document
}

func NewLSH(opt *Options) (*LSH, error) {
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
	l.Docs = make(map[uint64]*Document)
	return l, nil
}

// Index stores the document in the LSH data structure. Returns an error if the document
// is already present and will attempt to rollback any changes to other tables.
func (l *LSH) Index(d *Document) error {
	if len(d.Features) != l.Opt.NumFeatures {
		return errInvalidDocument
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

// Search looks through and merges results from all tables to find the nearest neighbors to the
// provided feature vector
func (l *LSH) Search(f []float64, numToReturn int, threshold float64) (Scores, error) {
	if len(f) != l.Opt.NumFeatures {
		return nil, errInvalidDocument
	}

	floats.Scale(1.0/floats.Norm(f, 2), f)

	rbRes := roaring64.New()
	for _, t := range l.Tables {
		hash, err := t.Hyperplanes.hash(f)
		if err != nil {
			return nil, err
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

	res := NewResults(numToReturn, threshold, SignFilter_ANY)

	for _, uid := range rbRes.ToArray() {
		doc, exists := l.Docs[uid]
		if !exists || doc == nil {
			continue
		}
		score := stat.Correlation(f, doc.Features, nil)
		res.Update(Score{uid, score})
	}
	return res.Fetch(), nil
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
		return errDocumentExists
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
		return errDocumentNotStored
	}

	rb, exists := t.Table[hash]
	if !exists {
		return errHashNotFound
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
