package lsh

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
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
	_, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []SimpleDocument{
		{0, []float64{0, 0, 5}, nil},
		{1, []float64{0, 0.1, 3}, nil},
		{2, []float64{0, 0.1, 2}, nil},
		{3, []float64{0, 0.1, 1}, nil},
		{4, []float64{0, -0.1, -4}, nil},
	}
	for _, d := range docs {
		if err := lsh.Index(d, nil); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	scores, _, err := lsh.Search([]float64{0, 0, 0.1}, so)
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

	scores, _, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 3}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Index(NewSimpleDocument(2, []float64{0, 0.1, 2}, nil), nil); err != nil {
		t.Fatal(err)
	}
	scores, _, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_NEG
	scores, _, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{4}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_ANY
	scores, _, err = lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 4, 1}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.Threshold = 1
	scores, _, err = lsh.Search([]float64{0, 0, 0.1}, so)
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

	docs := []SimpleDocument{
		{0, []float64{0, 0, 5}, nil},
		{1, []float64{0, 0.1, 3}, nil},
		{2, []float64{0, 0.1, 2}, nil},
		{3, []float64{0, 0.1, 1}, nil},
	}
	for _, d := range docs {
		if err := lsh.Index(d, nil); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	scores, _, err := lsh.Search([]float64{0, 0, 0.1}, so)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	lshFile := "test.lsh"
	if err := lsh.Save(lshFile, SimpleDocument{}); err != nil {
		os.Remove(lshFile)
		t.Fatal(err)
	}
	defer os.Remove(lshFile)

	newLsh := new(LSH)
	if err := newLsh.Load(lshFile); err != nil {
		t.Fatal(err)
	}

	scores, _, err = newLsh.Search([]float64{0, 0, 0.1}, so)
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

func TestIndexSimple(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		doc         SimpleDocument
		expectedErr error
	}{
		{SimpleDocument{0, []float64{0, 1}, nil}, ErrInvalidDocument},
		{SimpleDocument{1, []float64{3, 3, 3}, nil}, ErrNoFeatureComplexity},
		{SimpleDocument{2, []float64{3, 3, 0}, nil}, nil},
		{SimpleDocument{2, []float64{1, 2, 3}, nil}, ErrDuplicateDocument},
	}
	for _, td := range testData {
		if err := lsh.Index(td.doc, nil); err != td.expectedErr {
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

	docs := []SimpleDocument{
		{0, []float64{0, 1, 3}, nil},
		{1, []float64{1, 3, 3}, nil},
		{2, []float64{3, 3, 0}, nil},
		{3, []float64{1, 2, 3}, nil},
	}

	for _, d := range docs {
		if err := lsh.Index(d, nil); err != nil {
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

	docs := []SimpleDocument{
		{0, []float64{0, 1, 3}, nil},
		{1, []float64{1, 3, 3}, nil},
		{2, []float64{3, 3, 0}, nil},
		{3, []float64{1, 2, 3}, nil},
	}

	docGroups := []SimpleDocument{
		{4, []float64{-7, 8, -9}, map[string]string{"service": "app1", "host": "host1"}},
		{5, []float64{-7, 9, -5.5}, map[string]string{"service": "app1", "host": "host2"}},
		{6, []float64{-7, 9, -7}, map[string]string{"service": "app2", "host": "host2"}},
		{7, []float64{-7, 10, -7}, map[string]string{"service": "app2", "host": "host3"}},
	}

	for _, d := range docs {
		if err := lsh.Index(d, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range docGroups {
		if err := lsh.Index(d, []string{"service", "host"}); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	if _, _, err := lsh.Search([]float64{1, 2}, so); err != ErrInvalidDocument {
		t.Fatalf("expected %v, but got %v error", ErrInvalidDocument, err)
	}

	so.NumToReturn = 0
	if _, _, err := lsh.Search([]float64{1, 2, 3}, so); err != ErrInvalidNumToReturn {
		t.Fatalf("expected %v, but got %v error", ErrInvalidNumToReturn, err)
	}

	so = NewDefaultSearchOptions()
	so.SignFilter = SignFilter_POS
	testData := []struct {
		f        []float64
		sf       SignFilter
		q        map[string][]string
		expected Scores
	}{
		{
			[]float64{0, 1, 3},
			SignFilter_POS,
			nil,
			Scores{
				{UID: 0, Score: 1.00},
				{UID: 3, Score: 0.98},
			},
		},
		{
			[]float64{-7, 9, -7},
			SignFilter_POS,
			nil,
			Scores{
				{UID: 6, Score: 1.00},
				{UID: 7, Score: 0.99},
				{UID: 5, Score: 0.99},
				{UID: 4, Score: 0.99},
			},
		},
		{
			[]float64{-7, 9, -7},
			SignFilter_POS,
			map[string][]string{"service": {"app1"}},
			Scores{
				{UID: 5, Score: 0.99},
				{UID: 4, Score: 0.99},
			},
		},
		{
			[]float64{-7, 9, -7},
			SignFilter_POS,
			map[string][]string{"service": {"app2"}},
			Scores{
				{UID: 6, Score: 1.00},
				{UID: 7, Score: 0.99},
			},
		},
		{
			[]float64{-7, 9, -7},
			SignFilter_POS,
			map[string][]string{"host": {"host2"}},
			Scores{
				{UID: 6, Score: 1.00},
				{UID: 5, Score: 0.99},
			},
		},
		{
			[]float64{-7, 9, -7},
			SignFilter_POS,
			map[string][]string{"host": {}},
			Scores{
				{UID: 6, Score: 1.00},
				{UID: 7, Score: 0.99},
				{UID: 5, Score: 0.99},
				{UID: 4, Score: 0.99},
			},
		},
	}

	for _, td := range testData {
		so.SignFilter = td.sf
		so.Query = td.q
		res, _, err := lsh.Search(td.f, so)
		if err != nil {
			t.Fatal(err)
		}
		if err := compareScores(res, td.expected); err != nil {
			t.Fatal(err)
		}
	}

}

func TestLSHError(t *testing.T) {
	numHyperplanes := 8
	numTables := 3
	numIter := 100
	numFeatures := 10
	numDocs := 100000
	threshold := 0.85

	opt := NewDefaultOptions()
	opt.NumHyperplanes = numHyperplanes
	opt.NumTables = numTables
	opt.NumFeatures = numFeatures

	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	features := make([][]float64, numDocs)
	for i := 0; i < numDocs; i++ {
		features[i] = make([]float64, numFeatures)
		for j := 0; j < numFeatures; j++ {
			features[i][j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(features[i], 2), features[i])
	}

	start := time.Now()
	for i, f := range features {
		if err := lsh.Index(NewSimpleDocument(uint64(i), f, nil), nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("index time: %v\n", time.Since(start))

	so := NewDefaultSearchOptions()
	so.NumToReturn = numDocs
	so.SignFilter = SignFilter_POS
	so.Threshold = threshold

	f := make([]float64, numFeatures)
	scored := make([]float64, 0, numIter)
	counts := make([]float64, 0, numIter)
	scores := make([]float64, 0, numIter)
	for i := 0; i < numIter; i++ {
		for j := 0; j < numFeatures; j++ {
			f[j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(f, 2), f)
		res, nscored, err := lsh.Search(f, so)
		if err != nil {
			t.Fatal(err)
		}
		scored = append(scored, float64(nscored))
		counts = append(counts, float64(len(res)))
		if len(res) > 0 {
			scores = append(scores, res[len(res)-1].Score)
		}
	}
	nsm, nsstd := stat.MeanStdDev(scored, nil)
	cm, cstd := stat.MeanStdDev(counts, nil)
	sm, sstd := stat.MeanStdDev(scores, nil)
	t.Logf("iterations: %d, num_scored: %d +/-%d, count: %d +/-%d, low_scores: %.3f +/-%.3f\n", numIter, int(nsm), int(nsstd), int(cm), int(cstd), sm, sstd)
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

func compareScores(res, expected Scores) error {
	if len(res) != len(expected) {
		return fmt.Errorf("expected %d scores, but got %d", len(expected), len(res))
	}
	for i, s := range expected {
		if s.UID != res[i].UID {
			return fmt.Errorf("expected uid %d, but got %d", s.UID, res[i].UID)
		}
		if math.Abs(s.Score-res[i].Score) > 0.01 {
			return fmt.Errorf("expected score %.2f, but got %.2f", s.Score, res[i].Score)
		}
	}
	return nil
}
