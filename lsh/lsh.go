package lsh

import (
	"errors"
	"math"
	"sync"

	"github.com/aouyang1/go-lsh/configs"
	"github.com/aouyang1/go-lsh/document"
	"github.com/aouyang1/go-lsh/forwardindex"
	"github.com/aouyang1/go-lsh/hyperplanes"
	"github.com/aouyang1/go-lsh/options"
	"github.com/aouyang1/go-lsh/results"
	"github.com/aouyang1/go-lsh/stats"
	"github.com/aouyang1/go-lsh/tables"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

var (
	ErrInvalidDocument    = errors.New("vector length does not match with the configured options")
	ErrNoOptions          = errors.New("no options set for LSH")
	ErrNoVectorComplexity = errors.New("vector does not have enough complexity with a standard deviation of 0")
)

// LSH represents the locality sensitive hash struct that stores the multiple tables containing
// the configured number of hyperplanes along with the documents currently indexed.
type LSH struct {
	Cfg    *configs.LSHConfigs
	Tables []*tables.Table        // N tables each using a different randomly generated set of hyperplanes
	Docs   *forwardindex.InMemory // forward index which may be offloaded to a separate system
}

// New returns a new Locality Sensitive Hash struct ready for indexing and searching
func New(cfg *configs.LSHConfigs) (*LSH, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	l := new(LSH)
	l.Cfg = cfg

	hyperplaneTables := make([]*hyperplanes.Hyperplanes, 0, cfg.NumTables)
	for i := 0; i < cfg.NumTables; i++ {
		ht, err := hyperplanes.New(l.Cfg.NumHyperplanes, l.Cfg.VectorLength)
		if err != nil {
			return nil, err
		}
		hyperplaneTables = append(hyperplaneTables, ht)
	}
	tables, err := tables.New(l.Cfg, hyperplaneTables)
	if err != nil {
		return nil, err
	}
	l.Tables = tables

	l.Docs = forwardindex.NewInMemory(l.Cfg)
	return l, nil
}

// Index stores the document in the LSH data structure. Returns an error if the document
// is already present.
func (l *LSH) Index(d document.Document) error {
	origDoc := d.Copy()
	vec := d.GetVector()
	if len(vec) != l.Cfg.VectorLength {
		return ErrInvalidDocument
	}
	if stat.StdDev(vec, nil) == 0 {
		return ErrNoVectorComplexity
	}

	vec = l.Cfg.TFunc(vec)

	if err := l.index(d); err != nil {
		return err
	}

	// expand current doc of the uid if present
	l.Docs.Index(origDoc)
	return nil
}

func (l *LSH) index(d document.Document) error {
	for _, t := range l.Tables {
		if err := t.Index(d); err != nil {
			return err
		}
	}
	return nil
}

// Delete attempts to remove the uid from the tables and also the document map
func (l *LSH) Delete(uid uint64) error {
	var err error
	for _, t := range l.Tables {
		if e := t.Delete(uid); e != nil {
			err = e
		}
	}
	l.Docs.Delete(uid)
	return err
}

// Search looks through and merges results from all tables to find the nearest neighbors to the
// provided vector
func (l *LSH) Search(d document.Document, s *options.Search) (results.Scores, int, error) {
	v := d.GetVector()
	if len(v) != l.Cfg.VectorLength {
		return nil, 0, ErrInvalidDocument
	}
	l.Cfg.TFunc(v)

	if s == nil {
		s = options.NewDefaultSearch()
	} else {
		if err := s.Validate(); err != nil {
			return nil, 0, err
		}
	}

	docIds, err := l.filterDocs(d, s)
	if err != nil {
		return nil, 0, err
	}
	res := results.New(s.NumToReturn, s.Threshold, s.SignFilter)
	l.score(d, docIds, res)

	return res.Fetch(), res.NumScored, nil
}

// Filter returns a set of document ids that match the given vector and search options
func (l *LSH) filterDocs(d document.Document, s *options.Search) (map[uint64]map[int64]struct{}, error) {
	vec := d.GetVector()
	if len(vec) != l.Cfg.VectorLength {
		return nil, ErrInvalidDocument
	}

	if s == nil {
		s = options.NewDefaultSearch()
	} else {
		if err := s.Validate(); err != nil {
			return nil, err
		}
	}

	docIds := make(map[uint64]map[int64]struct{})
	// search for positively correlated results
	if s.SignFilter == options.SignFilter_ANY || s.SignFilter == options.SignFilter_POS {
		dids := l.filterDocsByLag(d, s.MaxLag)
		for uid, indexes := range dids {
			for index := range indexes {
				uidIndexes, exists := docIds[uid]
				if !exists {
					uidIndexes = make(map[int64]struct{})
					docIds[uid] = uidIndexes
				}
				uidIndexes[index] = struct{}{}
			}
		}
	}

	// search for negatively correlated results
	if s.SignFilter == options.SignFilter_ANY || s.SignFilter == options.SignFilter_NEG {
		floats.Scale(-1, vec)
		dids := l.filterDocsByLag(d, s.MaxLag)
		floats.Scale(-1, vec) // undo negation
		for uid, indexes := range dids {
			for index := range indexes {
				uidIndexes, exists := docIds[uid]
				if !exists {
					uidIndexes = make(map[int64]struct{})
					docIds[uid] = uidIndexes
				}
				uidIndexes[index] = struct{}{}
			}
		}
	}

	return docIds, nil
}

func (l *LSH) filterDocsByLag(d document.Document, maxLag int64) map[uint64]map[int64]struct{} {
	mergedRes := make(map[uint64]map[int64]struct{})
	var resLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(l.Tables))

	for _, t := range l.Tables {
		go func(tbl *tables.Table) {
			defer wg.Done()
			docToIndex := tbl.Filter(d, maxLag)
			resLock.Lock()
			for uid, indexes := range docToIndex {
				for index := range indexes {
					uidIndexes, exists := mergedRes[uid]
					if !exists {
						uidIndexes = make(map[int64]struct{})
						mergedRes[uid] = uidIndexes
					}
					uidIndexes[index] = struct{}{}
				}
			}
			resLock.Unlock()
		}(t)
	}
	wg.Wait()

	return mergedRes
}

// Score takes a set of document ids and scores them against a provided search query
func (l *LSH) score(d document.Document, docIds map[uint64]map[int64]struct{}, res *results.Results) {
	for uid, indexes := range docIds {
		for index := range indexes {
			currDocVec := l.Docs.GetVector(uid, index)
			if currDocVec == nil {
				continue
			}
			l.Cfg.TFunc(currDocVec)
			score := stat.Correlation(d.GetVector(), currDocVec, nil)
			res.Update(results.Score{UID: uid, Index: index, Score: score})
		}
	}
}

// TODO: this needs more thought
// Save takes a filepath and a document interface representing the indexed documents
// and saves the lsh index to disk. Only one type of document is currently supported
// which will be registered with gob to encode and save to disk.
/*
func (l *LSH) Save(filepath string, d document.Document) error {
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
*/

// Stats returns the current statistics about the configured LSH struct.
func (l *LSH) Stats() *stats.Statistics {
	s := new(stats.Statistics)
	s.NumDocs = l.Docs.Size()

	thetaInc := 0.05
	thetaStart := 0.60
	thetaEnd := 1.0

	// compute false negative errors for various thresholds
	s.FalseNegativeErrors = make([]stats.FalseNegativeError, 0, int((thetaEnd-thetaStart)/thetaInc))
	for theta := thetaStart; theta < thetaEnd; theta += thetaInc {
		pdiff := 2 / math.Pi * math.Acos(theta)
		psame := 1 - pdiff

		fneg := math.Pow((1 - math.Pow(psame, float64(l.Cfg.NumHyperplanes))), float64(l.Cfg.NumTables))

		fnegErr := stats.FalseNegativeError{Threshold: theta, Probability: fneg}
		s.FalseNegativeErrors = append(s.FalseNegativeErrors, fnegErr)
	}
	return s
}
