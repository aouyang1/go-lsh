package lsh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"

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
		NumHyperplanes: 16,
		NumTables:      2,
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
	opt    *Options
	tables []*table
}

func NewLSH(opt *Options) (*LSH, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.opt = opt

	var err error

	l.tables = make([]*table, l.opt.NumTables)
	for i := 0; i < l.opt.NumTables; i++ {
		l.tables[i], err = newTable(l.opt.NumHyperplanes, l.opt.NumFeatures)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (l *LSH) Index(d *Document) error {
	if len(d.features) != l.opt.NumFeatures {
		return errInvalidDocument
	}

	floats.Scale(1.0/floats.Norm(d.features, 2), d.features)

	for _, t := range l.tables {
		if err := t.index(d); err != nil {
			return err
		}
	}
	return nil
}

func (l *LSH) Delete(uid uint64) error {
	var err error
	for _, t := range l.tables {
		if e := t.delete(uid); e != nil {
			err = e
		}
	}
	return err
}

func (l *LSH) Search(f []float64, numToReturn int, threshold float64) ([]uint64, error) {
	if len(f) != l.opt.NumFeatures {
		return nil, errInvalidDocument
	}

	floats.Scale(1.0/floats.Norm(f, 2), f)

	res := make(map[uint64]struct{})
	for _, t := range l.tables {
		uids, err := t.search(f, numToReturn, threshold)
		if err != nil {
			return nil, err
		}
		for _, uid := range uids {
			if _, exists := res[uid]; !exists {
				res[uid] = struct{}{}
			}
		}
	}

	var out []uint64
	for uid := range res {
		out = append(out, uid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

type hyperplanes struct {
	planes [][]float64
	buffer []byte
}

func newHyperplanes(numHyperplanes, numFeatures int) (*hyperplanes, error) {
	if numHyperplanes < 1 {
		return nil, errInvalidNumHyperplanes
	}

	if numFeatures < 1 {
		return nil, errInvalidNumFeatures
	}

	h := new(hyperplanes)
	h.buffer = make([]byte, 8)
	h.planes = make([][]float64, numHyperplanes)
	for i := 0; i < numHyperplanes; i++ {
		h.planes[i] = make([]float64, numFeatures)
		for j := 0; j < numFeatures; j++ {
			h.planes[i][j] = rand.Float64()
		}
		floats.Scale(1/floats.Norm(h.planes[i], 2), h.planes[i])
	}

	return h, nil
}

func (h *hyperplanes) hash(f []float64) (uint64, error) {
	if len(f) == 0 {
		return 0, errNoFeatures
	}

	bs := h.buffer
	var b byte
	var bitCnt, byteCnt int

	for _, p := range h.planes {
		if len(f) != len(p) {
			return 0, fmt.Errorf("%v, has length %d when expecting length, %d", errFeatureLengthMismatch, len(f), len(p))
		}
		if floats.Dot(p, f) > 0 {
			b = b | byte(1)<<(8-bitCnt-1)
		}
		bitCnt++
		if bitCnt == 8 {
			bs[byteCnt] = b
			bitCnt = 0
			b = 0
			byteCnt++
		}
	}

	// didn't fill a full byte
	if bitCnt != 0 {
		bs[byteCnt] = b
	}
	return binary.BigEndian.Uint64(bs), nil
}

type Document struct {
	uid      uint64
	features []float64
}

func NewDocument(uid uint64, f []float64) *Document {
	return &Document{
		uid:      uid,
		features: f,
	}
}

type table struct {
	hyperplanes *hyperplanes
	table       map[uint64][]uint64
	docs        map[uint64]*tableDoc
}

func newTable(numHyper, numFeat int) (*table, error) {
	t := new(table)

	var err error
	t.hyperplanes, err = newHyperplanes(numHyper, numFeat)
	if err != nil {
		return nil, err
	}

	t.table = make(map[uint64][]uint64)
	t.docs = make(map[uint64]*tableDoc)
	return t, nil
}

func (t *table) index(d *Document) error {
	if _, exists := t.docs[d.uid]; exists {
		return errDocumentExists
	}

	hash, err := t.hyperplanes.hash(d.features)
	if err != nil {
		return err
	}
	uids := t.table[hash]

	if len(uids) == 0 {
		t.table[hash] = []uint64{d.uid}
	} else {
		// insert in sorted uid order
		for i := 0; i < len(uids)-1; i++ {
			if d.uid > uids[i] && d.uid < uids[i+1] {
				uids = append(uids, 0)
				copy(uids[i+2:], uids[i+1:len(uids)-1])
				uids[i+1] = d.uid
				t.table[hash] = uids
				t.docs[d.uid] = newTableDoc(d, hash)
				return nil
			}
		}

		// uid is greater than all uids in key
		t.table[hash] = append(t.table[hash], d.uid)
	}
	t.docs[d.uid] = newTableDoc(d, hash)
	return nil
}

func (t *table) delete(uid uint64) error {
	tdoc, exists := t.docs[uid]
	if !exists {
		return errDocumentNotStored
	}
	hash := tdoc.hash

	uids, exists := t.table[hash]
	if !exists {
		return errHashNotFound
	}

	for i := 0; i < len(uids); i++ {
		if uids[i] == uid {
			uids[i] = uids[len(uids)-1]
			if len(uids) == 1 {
				delete(t.table, hash)
			} else {
				t.table[hash] = uids[:len(uids)-1]
			}
			delete(t.docs, uid)
			return nil
		}
	}
	return errDocumentNotStored
}

func (t *table) search(f []float64, numToReturn int, threshold float64) ([]uint64, error) {
	hash, err := t.hyperplanes.hash(f)
	if err != nil {
		return nil, err
	}
	uids := t.table[hash]
	out := make([]uint64, 0, numToReturn)
	for _, uid := range uids {
		tdoc, exists := t.docs[uid]
		if !exists || tdoc == nil {
			continue
		}
		score := stat.Correlation(f, tdoc.features(), nil)
		if math.Abs(score) >= threshold {
			out = append(out, uid)
		}
	}
	if len(out) > numToReturn {
		return out[:numToReturn], nil
	}
	return out, nil
}

type tableDoc struct {
	hash uint64
	doc  *Document
}

func newTableDoc(d *Document, hash uint64) *tableDoc {
	return &tableDoc{hash, d}
}

func (t *tableDoc) features() []float64 {
	if t == nil {
		return nil
	}
	if t.doc == nil {
		return nil
	}
	return t.doc.features
}
