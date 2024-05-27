package tables

import (
	"errors"
	"strconv"

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
		tables[i], err = NewTable(strconv.Itoa(i), ht[i], cfg)
		if err != nil {
			return nil, err
		}
	}
	return tables, err
}

// Table maps buckets to a bitmap of document ids. Where documents are stored in the table is determined by
// finding the bucket a document is mapped to.
type Table struct {
	Name string
	Cfg  *configs.LSHConfigs

	Hyperplanes *hyperplanes.Hyperplanes
	Table       map[int64]map[uint16]*bitmap.Bitmap // row index to hash to bitmaps
	Doc2Hash    map[uint64]map[uint16][]int64       // uid to hash to slice of timestamps
}

func NewTable(name string, h *hyperplanes.Hyperplanes, cfg *configs.LSHConfigs) (*Table, error) {
	t := new(Table)
	t.Name = name
	t.Cfg = cfg

	var err error
	t.Hyperplanes = h
	if err != nil {
		return nil, err
	}

	t.Table = make(map[int64]map[uint16]*bitmap.Bitmap)
	t.Doc2Hash = make(map[uint64]map[uint16][]int64)
	return t, nil
}

func (t *Table) Index(d document.Document) error {
	uid := d.GetUID()
	v := d.GetVector()

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

	rb.Add(uid)

	hashTimestamps, exists := t.Doc2Hash[uid]
	if !exists {
		hashTimestamps = make(map[uint16][]int64)
		t.Doc2Hash[uid] = hashTimestamps
	}
	timestamps := hashTimestamps[hash]
	timestamps = append(timestamps, d.GetIndex())
	hashTimestamps[hash] = timestamps
	return nil
}

func (t *Table) Filter(d document.Document, maxLag int64) map[uint64]map[int64]struct{} {
	v := d.GetVector()
	hash, _ := t.Hyperplanes.Hash16(v)
	docToIndex := make(map[uint64]map[int64]struct{})
	if maxLag > -1 {
		// indicates we're looking for time windows with some wiggle room
		startIdx := d.GetIndex() - maxLag
		endIdx := d.GetIndex() + maxLag
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
			for _, uid := range rb.Rb.ToArray() {
				indexMap, exists := docToIndex[uid]
				if !exists {
					indexMap = make(map[int64]struct{})
					docToIndex[uid] = indexMap
				}
				for _, index := range t.Doc2Hash[uid][hash] {
					// keep only indexes within the specified lag
					if index >= startIdx && index <= endIdx {
						indexMap[index] = struct{}{}
					}
				}
			}
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
			for _, uid := range rb.Rb.ToArray() {
				indexMap, exists := docToIndex[uid]
				if !exists {
					indexMap = make(map[int64]struct{})
					docToIndex[uid] = indexMap
				}
				for _, index := range t.Doc2Hash[uid][hash] {
					indexMap[index] = struct{}{}
				}
			}
			rb.Unlock()
		}
	}
	return docToIndex
}

func (t *Table) Delete(uid uint64) error {
	hashes, exists := t.Doc2Hash[uid]
	if !exists {
		return lsherrors.DocumentNotStored
	}

	err := ErrHashNotFound
	for _, tbl := range t.Table {
		for hash := range hashes {
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
	}
	delete(t.Doc2Hash, uid)
	return err
}
