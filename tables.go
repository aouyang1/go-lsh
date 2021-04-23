package lsh

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func NewTables(opt *Options) ([]*Table, error) {
	var err error

	tables := make([]*Table, opt.NumTables)
	for i := 0; i < opt.NumTables; i++ {
		tables[i], err = NewTable(opt.NumHyperplanes, opt.NumFeatures)
		if err != nil {
			return nil, err
		}
	}
	return tables, err
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

func (t *Table) index(d Document) error {
	uid := d.GetUID()
	feat := d.GetFeatures()
	if _, exists := t.Doc2Hash[uid]; exists {
		return ErrDuplicateDocument
	}

	hash, err := t.Hyperplanes.hash(feat)
	if err != nil {
		return err
	}
	rb, exists := t.Table[hash]
	if !exists || rb == nil {
		rb = newBitmap()
		t.Table[hash] = rb
	}

	if !rb.CheckedAdd(uid) {
		return fmt.Errorf("unable to add %d to bitmap at hash, %d", uid, hash)
	}

	t.Doc2Hash[uid] = hash
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
