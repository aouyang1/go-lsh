package lsh

import (
	"fmt"
	"os"
	"testing"
)

func TestNewOptions(t *testing.T) {
	testData := []struct {
		nf int
		nh int
		nt int

		err error
	}{
		{1, 1, 1, nil},
		{3, 5, 2, nil},
		{0, 0, 0, errInvalidNumHyperplanes},
		{3, 65, 2, errExceededMaxNumHyperplanes},
		{0, 5, 2, errInvalidNumFeatures},
		{3, 5, 0, errInvalidNumTables},
	}
	for _, td := range testData {
		opt := &Options{td.nh, td.nt, td.nf}
		if err := opt.Validate(); err != td.err {
			t.Errorf("expected %v, but got %v", td.err, err)
			continue
		}
	}
}

func TestNewLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := NewLSH(opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(lsh.Tables) != opt.NumTables {
		t.Fatalf("expected %d tables, but got %d", opt.NumTables, len(lsh.Tables))
	}
}

func TestLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := NewLSH(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 0, 5}},
		{1, []float64{0, 0.1, 3}},
		{2, []float64{0, 0.1, 2}},
		{3, []float64{0, 0.1, 1}},
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	scores, err := lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Delete(2); err != nil {
		t.Fatal(err)
	}

	scores, err = lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 3}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Index(NewDocument(2, []float64{0, 0.1, 2})); err != nil {
		t.Fatal(err)
	}
	scores, err = lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

}

func TestSaveLoadLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := NewLSH(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 0, 5}},
		{1, []float64{0, 0.1, 3}},
		{2, []float64{0, 0.1, 2}},
		{3, []float64{0, 0.1, 1}},
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	scores, err := lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	lshFile := "test.lsh"
	if err := lsh.Save(lshFile); err != nil {
		if _, serr := os.Stat(lshFile); serr == nil {
			if rerr := os.Remove(lshFile); rerr != nil {
				t.Fatalf("%v, %v", err, rerr)
			}
		}
		t.Fatal(err)
	}

	newLsh := new(LSH)
	if err := newLsh.Load(lshFile); err != nil {
		t.Fatal(err)
	}

	scores, err = newLsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

}
func compareUint64s(expected, uids []uint64) error {
	if len(uids) != len(expected) {
		return fmt.Errorf("expected %d results, but got %d", len(expected), len(uids))
	}
	for i, uid := range uids {
		if uid != expected[i] {
			return fmt.Errorf("expected %v, but got %v", expected, uids)
		}
	}
	return nil
}
