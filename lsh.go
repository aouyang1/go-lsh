package lsh

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"

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
	opt    *Options
	tables []*table
	docs   map[uint64]*Document
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
	l.docs = make(map[uint64]*Document)
	return l, nil
}

func (l *LSH) Index(d *Document) error {
	if len(d.features) != l.opt.NumFeatures {
		return errInvalidDocument
	}

	floats.Scale(1.0/floats.Norm(d.features, 2), d.features)

	for i, t := range l.tables {
		if err := t.index(d); err != nil {
			// attempt to roll back removing added document to other tables
			var derr error
			for j := 0; j < i; j++ {
				if e := l.tables[j].delete(d.uid); e != nil {
					derr = e
				}
			}
			return fmt.Errorf("%v, %v", err, derr)
		}
	}
	l.docs[d.uid] = d
	return nil
}

// Delete attempts to remove the uid from the tables and also the document map
func (l *LSH) Delete(uid uint64) error {
	var err error
	for _, t := range l.tables {
		if e := t.delete(uid); e != nil {
			err = e
		}
	}
	delete(l.docs, uid)
	return err
}

func (l *LSH) Search(f []float64, numToReturn int, threshold float64) (Scores, error) {
	if len(f) != l.opt.NumFeatures {
		return nil, errInvalidDocument
	}

	floats.Scale(1.0/floats.Norm(f, 2), f)

	rbRes := roaring64.New()
	for _, t := range l.tables {
		hash, err := t.hyperplanes.hash(f)
		if err != nil {
			return nil, err
		}
		rb := t.table[hash]
		if rb == nil {
			// feature vector hash not present in hyperplane partition
			continue
		}
		rbRes.Or(rb)
	}

	res := NewResults(numToReturn, threshold, SignFilter_ANY)

	for _, uid := range rbRes.ToArray() {
		doc, exists := l.docs[uid]
		if !exists || doc == nil {
			continue
		}
		score := stat.Correlation(f, doc.Features(), nil)
		res.Update(Score{uid, score})
	}
	return res.Fetch(), nil
}

type Score struct {
	UID   uint64  `json:"uid"`
	Score float64 `json:"score"`
}

// Scores is a slice of individual Score's
type Scores []Score

func (s Scores) Len() int {
	return len(s)
}

func (s Scores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Scores) Less(i, j int) bool {
	return math.Abs(s[i].Score) < math.Abs(s[j].Score)
}

// Push implements the function in the heap interface
func (s *Scores) Push(x interface{}) {
	*s = append(*s, x.(Score))
}

// Pop implements the function in the heap interface
func (s *Scores) Pop() interface{} {
	x := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return x
}

func (s Scores) UIDs() []uint64 {
	out := make([]uint64, 0, len(s))
	for _, score := range s {
		out = append(out, score.UID)
	}
	return out
}

func (s Scores) Scores() []float64 {
	out := make([]float64, 0, len(s))
	for _, score := range s {
		out = append(out, score.Score)
	}
	return out
}

type Results struct {
	TopN       int
	Threshold  float64
	SignFilter SignFilter
	scores     Scores
}

type SignFilter int

const (
	SignFilter_POS = 1
	SignFilter_NEG = -1
	SignFilter_ANY = 0
)

// NewResults creates a new instance of results to track these similar features
func NewResults(topN int, threshold float64, signFilter SignFilter) *Results {
	scores := make(Scores, 0, topN)

	// Build priority queue of size TopN so that we don't have to sort over the entire
	// score output
	heap.Init(&scores)

	return &Results{
		TopN:       topN,
		Threshold:  threshold,
		SignFilter: signFilter,
		scores:     scores,
	}
}

// passed checks if the input score satisfies the Results lag and threshold requirements
func (r *Results) passed(s Score) bool {
	return math.Abs(float64(s.Score)) >= r.Threshold &&
		(r.SignFilter == SignFilter_ANY ||
			(s.Score > 0 && r.SignFilter == SignFilter_POS) ||
			(s.Score < 0 && r.SignFilter == SignFilter_NEG))
}

// Update records the input score
func (r *Results) Update(s Score) {
	if !r.passed(s) {
		return
	}
	if r.scores.Len() == r.TopN {
		if math.Abs(s.Score) > math.Abs(r.scores[0].Score) {
			heap.Pop(&r.scores)
			heap.Push(&r.scores, s)
		}
	} else {
		heap.Push(&r.scores, s)
	}
}

// Fetch returns the sorted scores in ascending order
func (r *Results) Fetch() Scores {
	s := make(Scores, len(r.scores))
	var score Score
	numScores := len(r.scores)

	for i := numScores - 1; i >= 0; i-- {
		score = heap.Pop(&r.scores).(Score)
		s[i] = score
	}
	return s
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

func (d *Document) Features() []float64 {
	return d.features
}

func (d *Document) UID() uint64 {
	return d.uid
}

type table struct {
	hyperplanes *hyperplanes
	table       map[uint64]*roaring64.Bitmap
	doc2hash    map[uint64]uint64
}

func newTable(numHyper, numFeat int) (*table, error) {
	t := new(table)

	var err error
	t.hyperplanes, err = newHyperplanes(numHyper, numFeat)
	if err != nil {
		return nil, err
	}

	t.table = make(map[uint64]*roaring64.Bitmap)
	t.doc2hash = make(map[uint64]uint64)
	return t, nil
}

func (t *table) index(d *Document) error {
	if _, exists := t.doc2hash[d.uid]; exists {
		return errDocumentExists
	}

	hash, err := t.hyperplanes.hash(d.features)
	if err != nil {
		return err
	}
	rb, exists := t.table[hash]
	if !exists || rb == nil {
		rb = roaring64.New()
		t.table[hash] = rb
	}

	if !rb.CheckedAdd(d.uid) {
		return fmt.Errorf("unable to add %d to bitmap at hash, %d", d.uid, hash)
	}

	t.doc2hash[d.uid] = hash
	return nil
}

func (t *table) delete(uid uint64) error {
	hash, exists := t.doc2hash[uid]
	if !exists {
		return errDocumentNotStored
	}

	rb, exists := t.table[hash]
	if !exists {
		return errHashNotFound
	}

	if !rb.CheckedRemove(uid) {
		return fmt.Errorf("unable to remove %d to bitmap at hash, %d", uid, hash)
	}

	if rb.IsEmpty() {
		delete(t.table, hash)
	}
	delete(t.doc2hash, uid)
	return nil
}
