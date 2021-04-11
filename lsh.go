package lsh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"

	"gonum.org/v1/gonum/floats"
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
)

type Options struct {
	NumHyperplanes int
	NumTables      int
	NumFeatures    int
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

func NewDefaultOptions() *Options {
	return &Options{
		NumHyperplanes: 16,
		NumTables:      2,
		NumFeatures:    3,
	}
}

type LSH struct {
	opt    *Options
	tables []*Table
}

func (l *LSH) init() error {
	if l.opt == nil {
		return errNoOptions
	}

	var err error

	l.tables = make([]*Table, l.opt.NumTables)
	for i := 0; i < l.opt.NumTables; i++ {
		l.tables[i], err = NewTable(l.opt.NumHyperplanes, l.opt.NumFeatures)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LSH) Index(d *Document) error {
	if len(d.features) != l.opt.NumFeatures {
		return errInvalidDocument
	}

	floats.Scale(floats.Norm(d.features, 2), d.features)

	for _, t := range l.tables {
		if err := t.index(d); err != nil {
			return err
		}
	}
	return nil
}

func NewLSH(opt *Options) (*LSH, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.opt = opt
	if err := l.init(); err != nil {
		return nil, err
	}
	return l, nil
}

type Hyperplanes struct {
	planes [][]float64
}

func (h *Hyperplanes) hash(f []float64) (uint64, error) {
	if len(f) == 0 {
		return 0, errNoFeatures
	}

	var bs []byte
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
	return binary.LittleEndian.Uint64(bs), nil
}

func NewHyperplanes(numHyperplanes, numFeatures int) (*Hyperplanes, error) {
	if numHyperplanes < 1 {
		return nil, errInvalidNumHyperplanes
	}

	if numFeatures < 1 {
		return nil, errInvalidNumFeatures
	}

	h := new(Hyperplanes)
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

type Table struct {
	hyperplanes *Hyperplanes
	table       map[uint64][]uint64
}

func (t *Table) index(d *Document) error {
	hash, err := t.hyperplanes.hash(d.features)
	if err != nil {
		return err
	}
	t.table[hash] = append(t.table[hash], d.uid)
	return nil
}

func NewTable(numHyper, numFeat int) (*Table, error) {
	t := new(Table)

	var err error
	t.hyperplanes, err = NewHyperplanes(numHyper, numFeat)
	if err != nil {
		return nil, err
	}

	t.table = make(map[uint64][]uint64)
	return t, nil
}
