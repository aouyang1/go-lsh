package lsh

import (
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
)

var (
	ErrNoHyperplanes              = errors.New("no hyperplanes provided to creation of new tables")
	ErrTableToHyperplanesMismatch = errors.New("number of hyperplane tables does not match configured tables in options")
)

func NewTables(opt *Options, ht []*Hyperplanes) ([]*Table, error) {
	var err error
	if ht == nil {
		return nil, ErrNoHyperplanes
	}
	if len(ht) != opt.NumTables {
		return nil, ErrTableToHyperplanesMismatch
	}

	tables := make([]*Table, opt.NumTables)
	for i := 0; i < opt.NumTables; i++ {
		tables[i], err = NewTable(ht[i], opt)
		if err != nil {
			return nil, err
		}
	}
	return tables, err
}

// Table maps buckets to a bitmap of document ids. Where documents are stored in the table is determined by
// finding the bucket a document is mapped to.
type Table struct {
	Opt *Options

	Hyperplanes *Hyperplanes
	Table       map[int64]map[uint16]*Bitmap // row index to hash to bitmaps
	Doc2Hash    map[uint64]uint16
}

func NewTable(h *Hyperplanes, opt *Options) (*Table, error) {
	t := new(Table)
	t.Opt = opt

	var err error
	t.Hyperplanes = h
	if err != nil {
		return nil, err
	}

	t.Table = make(map[int64]map[uint16]*Bitmap)
	t.Doc2Hash = make(map[uint64]uint16)
	return t, nil
}

func (t *Table) index(d Document) error {
	uid := d.GetUID()
	v := d.GetVector()
	if _, exists := t.Doc2Hash[uid]; exists {
		return ErrDuplicateDocument
	}

	hash, err := t.Hyperplanes.Hash16(v)
	if err != nil {
		return err
	}

	rowIndex := d.GetIndex() / t.Opt.RowSize * t.Opt.RowSize

	tbl, exists := t.Table[rowIndex]
	if !exists {
		tbl = make(map[uint16]*Bitmap)
		t.Table[rowIndex] = tbl
	}
	rb, exists := tbl[hash]
	if !exists || rb == nil {
		rb = newBitmap()
		tbl[hash] = rb
	}

	if !rb.CheckedAdd(uid) {
		return fmt.Errorf("unable to add %d to bitmap at hash, %d", uid, hash)
	}

	t.Doc2Hash[uid] = hash
	return nil
}

func (t *Table) filter(d Document, maxLag int64) *roaring64.Bitmap {
	v := d.GetVector()
	hash, _ := t.Hyperplanes.Hash16(v)

	rbRes := roaring64.New()

	if maxLag > -1 {
		// indicates we're looking for time windows with some wiggle room
		startIdx := d.GetIndex()
		endIdx := startIdx + int64(t.Opt.VectorLength)*t.Opt.SamplePeriod
		startIdx -= maxLag
		endIdx += maxLag
		startRow := startIdx / t.Opt.RowSize * t.Opt.RowSize
		endRow := endIdx / t.Opt.RowSize * t.Opt.RowSize
		rows := (endRow-startRow)/t.Opt.RowSize + 1
		for i := int64(0); i < rows; i++ {
			tblRow, exists := t.Table[startRow+i*t.Opt.RowSize]
			if !exists {
				continue
			}
			rb := tblRow[hash]
			if rb == nil {
				continue
			}
			rb.mu.Lock()
			rbRes.Or(rb.Rb)
			rb.mu.Unlock()
		}
	} else {
		// scan for all
		for _, tblRow := range t.Table {
			rb := tblRow[hash]
			if rb == nil {
				continue
			}
			rb.mu.Lock()
			rbRes.Or(rb.Rb)
			rb.mu.Unlock()
		}
	}
	return rbRes
}

func (t *Table) delete(uid uint64) error {
	hash, exists := t.Doc2Hash[uid]
	if !exists {
		return ErrDocumentNotStored
	}

	err := ErrHashNotFound
	for _, tbl := range t.Table {
		rb, exists := tbl[hash]
		if !exists {
			continue
		}
		err = nil

		rb.CheckedRemove(uid)

		if rb.IsEmpty() {
			delete(tbl, hash)
		}
	}
	delete(t.Doc2Hash, uid)
	return err
}
