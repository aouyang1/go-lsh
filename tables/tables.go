package tables

import (
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/aouyang1/go-lsh/bitmap"
	"github.com/aouyang1/go-lsh/configs"
	"github.com/aouyang1/go-lsh/document"
	"github.com/aouyang1/go-lsh/hyperplanes"
	"github.com/aouyang1/go-lsh/lsherrors"
)

var (
	ErrNoHyperplanes              = errors.New("no hyperplanes provided to creation of new tables")
	ErrTableToHyperplanesMismatch = errors.New("number of hyperplane tables does not match configured tables in options")
	ErrHashNotFound               = errors.New("hash not found in table")
)

func New(cfg *configs.LSHConfigs, ht []*hyperplanes.Hyperplanes) ([]*Table, error) {
	var err error
	if ht == nil {
		return nil, ErrNoHyperplanes
	}
	if len(ht) != cfg.NumTables {
		return nil, ErrTableToHyperplanesMismatch
	}

	tables := make([]*Table, cfg.NumTables)
	for i := 0; i < cfg.NumTables; i++ {
		tables[i], err = NewTable(ht[i], cfg)
		if err != nil {
			return nil, err
		}
	}
	return tables, err
}

// Table maps buckets to a bitmap of document ids. Where documents are stored in the table is determined by
// finding the bucket a document is mapped to.
type Table struct {
	Cfg *configs.LSHConfigs

	Hyperplanes *hyperplanes.Hyperplanes
	Table       map[int64]map[uint16]*bitmap.Bitmap // row index to hash to bitmaps
	Doc2Hash    map[uint64]uint16
}

func NewTable(h *hyperplanes.Hyperplanes, cfg *configs.LSHConfigs) (*Table, error) {
	t := new(Table)
	t.Cfg = cfg

	var err error
	t.Hyperplanes = h
	if err != nil {
		return nil, err
	}

	t.Table = make(map[int64]map[uint16]*bitmap.Bitmap)
	t.Doc2Hash = make(map[uint64]uint16)
	return t, nil
}

func (t *Table) Index(d document.Document) error {
	uid := d.GetUID()
	v := d.GetVector()
	if _, exists := t.Doc2Hash[uid]; exists {
		return lsherrors.DuplicateDocument
	}

	hash, err := t.Hyperplanes.Hash16(v)
	if err != nil {
		return err
	}

	rowIndex := d.GetIndex() / t.Cfg.RowSize * t.Cfg.RowSize

	tbl, exists := t.Table[rowIndex]
	if !exists {
		tbl = make(map[uint16]*bitmap.Bitmap)
		t.Table[rowIndex] = tbl
	}
	rb, exists := tbl[hash]
	if !exists || rb == nil {
		rb = bitmap.New()
		tbl[hash] = rb
	}

	if !rb.CheckedAdd(uid) {
		return fmt.Errorf("unable to add %d to bitmap at hash, %d", uid, hash)
	}

	t.Doc2Hash[uid] = hash
	return nil
}

func (t *Table) Filter(d document.Document, maxLag int64) *roaring64.Bitmap {
	v := d.GetVector()
	hash, _ := t.Hyperplanes.Hash16(v)

	rbRes := roaring64.New()

	if maxLag > -1 {
		// indicates we're looking for time windows with some wiggle room
		startIdx := d.GetIndex()
		endIdx := startIdx + int64(t.Cfg.VectorLength)*t.Cfg.SamplePeriod
		startIdx -= maxLag
		endIdx += maxLag
		startRow := startIdx / t.Cfg.RowSize * t.Cfg.RowSize
		endRow := endIdx / t.Cfg.RowSize * t.Cfg.RowSize
		rows := (endRow-startRow)/t.Cfg.RowSize + 1
		for i := int64(0); i < rows; i++ {
			tblRow, exists := t.Table[startRow+i*t.Cfg.RowSize]
			if !exists {
				continue
			}
			rb := tblRow[hash]
			if rb == nil {
				continue
			}
			rb.Lock()
			rbRes.Or(rb.Rb)
			rb.Unlock()
		}
	} else {
		// scan for all
		for _, tblRow := range t.Table {
			rb := tblRow[hash]
			if rb == nil {
				continue
			}
			rb.Lock()
			rbRes.Or(rb.Rb)
			rb.Unlock()
		}
	}
	return rbRes
}

func (t *Table) Delete(uid uint64) error {
	hash, exists := t.Doc2Hash[uid]
	if !exists {
		return lsherrors.DocumentNotStored
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
