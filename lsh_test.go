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
		{0, 0, 0, ErrInvalidNumHyperplanes},
		{3, 65, 2, ErrExceededMaxNumHyperplanes},
		{0, 5, 2, ErrInvalidNumFeatures},
		{3, 5, 0, ErrInvalidNumTables},
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
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(lsh.Tables) != opt.NumTables {
		t.Fatalf("expected %d tables, but got %d", opt.NumTables, len(lsh.Tables))
	}
}

func TestLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 0, 5}},
		{1, []float64{0, 0.1, 3}},
		{2, []float64{0, 0.1, 2}},
		{3, []float64{0, 0.1, 1}},
		{4, []float64{0, -0.1, -4}},
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	scores, err := lsh.Search([]float64{0, 0, 0.1}, so)
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

	scores, err = lsh.Search([]float64{0, 0, 0.1}, so)
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
	scores, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_NEG
	scores, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{4}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_ANY
	scores, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 4, 1}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.Threshold = 1
	scores, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

}

func TestSaveLoadLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
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

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	scores, err := lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	lshFile := "test.lsh"
	if err := lsh.Save(lshFile); err != nil {
		os.Remove(lshFile)
		t.Fatal(err)
	}
	defer os.Remove(lshFile)

	newLsh := new(LSH)
	if err := newLsh.Load(lshFile); err != nil {
		t.Fatal(err)
	}

	scores, err = newLsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}
}

func TestSearchOptionsValidate(t *testing.T) {
	testData := []struct {
		numToReturn int
		threshold   float64
		signFilter  SignFilter

		expectedErr error
	}{
		{0, 0.65, SignFilter_ANY, ErrInvalidNumToReturn},
		{1, 1.3, SignFilter_ANY, ErrInvalidThreshold},
		{1, 0.65, SignFilter(2), ErrInvalidSignFilter},
		{1, 0.65, SignFilter_ANY, nil},
	}

	for _, td := range testData {
		s := &SearchOptions{
			NumToReturn: td.numToReturn,
			Threshold:   td.threshold,
			SignFilter:  td.signFilter,
		}
		if err := s.Validate(); err != td.expectedErr {
			t.Errorf("expected %v, but got %v for error", td.expectedErr, err)
			continue
		}
	}
}

func TestIndex(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		doc         *Document
		expectedErr error
	}{
		{&Document{0, []float64{0, 1}}, ErrInvalidDocument},
		{&Document{1, []float64{3, 3, 3}}, ErrNoFeatureComplexity},
		{&Document{2, []float64{3, 3, 0}}, nil},
		{&Document{2, []float64{1, 2, 3}}, ErrDuplicateDocument},
	}
	for _, td := range testData {
		if err := lsh.Index(td.doc); err != td.expectedErr {
			t.Errorf("expected %v, but got %v for error", td.expectedErr, err)
		}
	}
}

func TestDelete(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 1, 3}},
		{1, []float64{1, 3, 3}},
		{2, []float64{3, 3, 0}},
		{3, []float64{1, 2, 3}},
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	if err := lsh.Delete(2); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Delete(2); err != ErrDocumentNotStored {
		t.Fatalf("expected %v but got %v error", ErrDocumentNotStored, err)
	}
}

func TestSearch(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 1, 3}},
		{1, []float64{1, 3, 3}},
		{2, []float64{3, 3, 0}},
		{3, []float64{1, 2, 3}},
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	if _, err := lsh.Search([]float64{1, 2}, so); err != ErrInvalidDocument {
		t.Fatalf("expected %v, but got %v error", ErrInvalidDocument, err)
	}

	so.NumToReturn = 0
	if _, err := lsh.Search([]float64{1, 2, 3}, so); err != ErrInvalidNumToReturn {
		t.Fatalf("expected %v, but got %v error", ErrInvalidNumToReturn, err)
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
