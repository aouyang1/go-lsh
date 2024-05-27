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
		sp int64
		rs int64

		err error
	}{
		{1, 1, 1, 1, 1, nil},
		{3, 5, 2, 60, 7200, nil},
		{0, 0, 0, 0, 0, ErrInvalidNumHyperplanes},
		{3, 65, 2, 0, 0, ErrExceededMaxNumHyperplanes},
		{0, 5, 2, 0, 0, ErrInvalidVectorLength},
		{3, 5, 0, 0, 0, ErrInvalidNumTables},
		{3, 5, 2, 0, 0, ErrInvalidSamplePeriod},
		{3, 5, 2, 60, 0, ErrInvalidRowSize},
	}
	for _, td := range testData {
		opt := &Options{td.nh, td.nt, td.nf, td.sp, td.rs, NewDefaultTransformFunc}
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

func TestLSHSearch(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []Document{
		NewSimpleDocument(0, 0, []float64{0, 0, 5}),
		NewSimpleDocument(1, 0, []float64{0, 0.1, 3}),
		NewSimpleDocument(2, 0, []float64{0, 0.1, 2}),
		NewSimpleDocument(3, 0, []float64{0, 0.1, 1}),
		NewSimpleDocument(4, 0, []float64{0, -0.1, -4}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	d := SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err := lsh.Search(d, so)
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

	d = SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 3}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Index(NewSimpleDocument(2, 0, []float64{0, 0.1, 2})); err != nil {
		t.Fatal(err)
	}

	d = SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_NEG
	d = SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{4}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = SignFilter_ANY
	d = SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 4, 1}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.Threshold = 1
	d = SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
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

	docs := []Document{
		NewSimpleDocument(0, 0, []float64{0, 0, 5}),
		NewSimpleDocument(1, 0, []float64{0, 0.1, 3}),
		NewSimpleDocument(2, 0, []float64{0, 0.1, 2}),
		NewSimpleDocument(3, 0, []float64{0, 0.1, 1}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	so.NumToReturn = 3
	so.SignFilter = SignFilter_POS

	d := SimpleDocument{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err := lsh.Search(d, so)
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
	newLsh.Opt.tFunc = NewDefaultTransformFunc
	d = SimpleDocument{Vector: []float64{0, 0, 0.1}}
	scores, _, err = newLsh.Search(d, so)
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
		doc         Document
		expectedErr error
	}{
		{NewSimpleDocument(0, 0, []float64{0, 1}), ErrInvalidDocument},
		{NewSimpleDocument(1, 0, []float64{3, 3, 3}), ErrNoVectorComplexity},
		{NewSimpleDocument(2, 0, []float64{3, 3, 0}), nil},
		{NewSimpleDocument(2, 0, []float64{1, 2, 3}), ErrDuplicateDocument},
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

	docs := []Document{
		NewSimpleDocument(0, 0, []float64{0, 1, 3}),
		NewSimpleDocument(1, 0, []float64{1, 3, 3}),
		NewSimpleDocument(2, 0, []float64{3, 3, 0}),
		NewSimpleDocument(3, 0, []float64{1, 2, 3}),
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

	docs := []Document{
		NewSimpleDocument(0, 0, []float64{0, 1, 3}),
		NewSimpleDocument(1, 0, []float64{1, 3, 3}),
		NewSimpleDocument(2, 0, []float64{3, 3, 0}),
		NewSimpleDocument(3, 0, []float64{1, 2, 3}),
	}

	docGroups := []Document{
		NewSimpleDocument(4, 0, []float64{-7, 8, -9}),
		NewSimpleDocument(5, 0, []float64{-7, 9, -5.5}),
		NewSimpleDocument(6, 0, []float64{-7, 9, -7}),
		NewSimpleDocument(7, 0, []float64{-7, 10, -7}),
		NewSimpleDocument(8, 0, []float64{-5, -3, -2}),
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range docGroups {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := NewDefaultSearchOptions()
	d := SimpleDocument{Vector: []float64{1, 2}}
	if _, _, err := lsh.Search(d, so); err != ErrInvalidDocument {
		t.Fatalf("expected %v, but got %v error", ErrInvalidDocument, err)
	}

	so.NumToReturn = 0
	d = SimpleDocument{Vector: []float64{1, 2, 3}}
	if _, _, err := lsh.Search(d, so); err != ErrInvalidNumToReturn {
		t.Fatalf("expected %v, but got %v error", ErrInvalidNumToReturn, err)
	}

	so = NewDefaultSearchOptions()
	so.SignFilter = SignFilter_POS
	testData := []struct {
		d        Document
		sf       SignFilter
		expected Scores
	}{
		{
			SimpleDocument{Vector: []float64{0, 1, 3}},
			SignFilter_POS,
			Scores{
				{UID: 0, Score: 1.00},
				{UID: 3, Score: 0.98},
			},
		},
		{
			SimpleDocument{Vector: []float64{-7, 9, -7}},
			SignFilter_POS,
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
		res, _, err := lsh.Search(td.d, so)
		if err != nil {
			t.Fatal(err)
		}
		if err := compareScores(res, td.expected); err != nil {
			t.Fatalf("%v, %v", err, td)
		}
	}

}

func TestLSHError(t *testing.T) {
	numHyperplanes := 8
	numTables := 3
	numIter := 100
	vecLen := 10
	numDocs := 100000
	threshold := 0.85

	opt := NewDefaultOptions()
	opt.NumHyperplanes = numHyperplanes
	opt.NumTables = numTables
	opt.VectorLength = vecLen

	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	vectors := make([][]float64, numDocs)
	for i := 0; i < numDocs; i++ {
		vectors[i] = make([]float64, vecLen)
		for j := 0; j < vecLen; j++ {
			vectors[i][j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(vectors[i], 2), vectors[i])
	}

	start := time.Now()
	for i, f := range vectors {
		if err := lsh.Index(NewSimpleDocument(uint64(i), 0, f)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("index time: %v\n", time.Since(start))

	so := NewDefaultSearchOptions()
	so.NumToReturn = numDocs
	so.SignFilter = SignFilter_POS
	so.Threshold = threshold

	f := make([]float64, vecLen)
	scored := make([]float64, 0, numIter)
	counts := make([]float64, 0, numIter)
	scores := make([]float64, 0, numIter)
	for i := 0; i < numIter; i++ {
		for j := 0; j < vecLen; j++ {
			f[j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(f, 2), f)
		d := SimpleDocument{Vector: f}
		res, nscored, err := lsh.Search(d, so)
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

func TestLSHStats(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []Document{
		NewSimpleDocument(0, 0, []float64{0, 0, 5}),
		NewSimpleDocument(1, 0, []float64{0, 0.1, 3}),
		NewSimpleDocument(2, 0, []float64{0, 0.1, 2}),
		NewSimpleDocument(3, 0, []float64{0, 0.1, 1}),
		NewSimpleDocument(4, 0, []float64{0, -0.1, -4}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	s := lsh.Stats()
	expectedS := &Statistics{
		NumDocs: len(docs),
		FalseNegativeErrors: []FalseNegativeError{
			{0.60, 0.903},
			{0.65, 0.804},
			{0.70, 0.636},
			{0.75, 0.395},
			{0.80, 0.149},
			{0.85, 0.018},
			{0.90, 0.000},
			{0.95, 0.000},
		},
	}
	if s.NumDocs != expectedS.NumDocs {
		t.Fatalf("expected %d, but got %d docs", expectedS.NumDocs, s.NumDocs)
	}
	if len(s.FalseNegativeErrors) != len(expectedS.FalseNegativeErrors) {
		t.Fatalf("expected %d, but got %d false negative errors", len(expectedS.FalseNegativeErrors), len(s.FalseNegativeErrors))
	}
	for i, fne := range s.FalseNegativeErrors {
		if math.Abs(fne.Threshold-expectedS.FalseNegativeErrors[i].Threshold) > 0.01 {
			t.Errorf("expected %.02f, but got %.02f threshold", expectedS.FalseNegativeErrors[i].Threshold, fne.Threshold)
		}
		if math.Abs(fne.Probability-expectedS.FalseNegativeErrors[i].Probability) > 0.001 {
			t.Errorf("expected %.03f, but got %.03f probability", expectedS.FalseNegativeErrors[i].Probability, fne.Probability)
		}
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

func BenchmarkLSHIndex(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		vec := make([]float64, opt.VectorLength)
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := NewSimpleDocument(uint64(i), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchSingleHyperplane(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	opt.NumHyperplanes = 1
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, opt.VectorLength)
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := NewSimpleDocument(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := make([]float64, opt.VectorLength)
	for j := 0; j < opt.VectorLength; j++ {
		query[j] = rand.Float64()
	}
	d := SimpleDocument{Vector: query}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchPositive(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, opt.VectorLength)
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := NewSimpleDocument(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := make([]float64, opt.VectorLength)
	for j := 0; j < opt.VectorLength; j++ {
		query[j] = rand.Float64()
	}
	d := SimpleDocument{Vector: query}
	so := NewDefaultSearchOptions()
	so.SignFilter = SignFilter_POS

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchRealistic(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	waveforms := make(map[string][]float64)
	spike := make([]float64, opt.VectorLength)
	spike[opt.VectorLength/2] = 1.0
	waveforms["spike"] = spike

	risingstep := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 2; i < opt.VectorLength; i++ {
		risingstep[i] = 1.0
	}
	waveforms["risingstep"] = risingstep

	loweringstep := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 2; i < opt.VectorLength; i++ {
		loweringstep[i] = -1.0
	}
	waveforms["loweringstep"] = loweringstep

	triangle := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 4; i < opt.VectorLength/2; i++ {
		triangle[i] = 1 * float64(i-opt.VectorLength/4)
	}
	for i := opt.VectorLength / 2; i < 3*opt.VectorLength/4; i++ {
		triangle[i] = -1*float64(i-opt.VectorLength/2) + 1
	}
	waveforms["triangle"] = triangle

	dip := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 4; i < opt.VectorLength/2; i++ {
		dip[i] = -1 * float64(i-opt.VectorLength/4)
	}
	for i := opt.VectorLength / 2; i < 3*opt.VectorLength/4; i++ {
		dip[i] = 1*float64(i-opt.VectorLength/2) - 1
	}
	waveforms["dip"] = dip

	waveNames := []string{"spike", "risingstep", "loweringstep", "triangle", "dip"}

	numDocuments := 100000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, opt.VectorLength)
		copy(vec, waveforms[waveNames[n%len(waveNames)]])
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] += rand.Float64()
		}
		doc := NewSimpleDocument(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := waveforms["risingstep"]
	d := SimpleDocument{Vector: query}

	so := NewDefaultSearchOptions()
	so.SignFilter = SignFilter_POS
	so.NumToReturn = 30000
	so.Threshold = 0.65

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 20000 {
			b.Fatalf("unexpected number of results, %d", len(res))
		}
	}
}

func BenchmarkLSHSearchRealisticSingleHyperplane(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	opt.NumHyperplanes = 1
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	waveforms := make(map[string][]float64)
	spike := make([]float64, opt.VectorLength)
	spike[opt.VectorLength/2] = 1.0
	waveforms["spike"] = spike

	risingstep := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 2; i < opt.VectorLength; i++ {
		risingstep[i] = 1.0
	}
	waveforms["risingstep"] = risingstep

	loweringstep := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 2; i < opt.VectorLength; i++ {
		loweringstep[i] = -1.0
	}
	waveforms["loweringstep"] = loweringstep

	triangle := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 4; i < opt.VectorLength/2; i++ {
		triangle[i] = 1 * float64(i-opt.VectorLength/4)
	}
	for i := opt.VectorLength / 2; i < 3*opt.VectorLength/4; i++ {
		triangle[i] = -1*float64(i-opt.VectorLength/2) + 1
	}
	waveforms["triangle"] = triangle

	dip := make([]float64, opt.VectorLength)
	for i := opt.VectorLength / 4; i < opt.VectorLength/2; i++ {
		dip[i] = -1 * float64(i-opt.VectorLength/4)
	}
	for i := opt.VectorLength / 2; i < 3*opt.VectorLength/4; i++ {
		dip[i] = 1*float64(i-opt.VectorLength/2) - 1
	}
	waveforms["dip"] = dip

	waveNames := []string{"spike", "risingstep", "loweringstep", "triangle", "dip"}

	numDocuments := 100000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, opt.VectorLength)
		copy(vec, waveforms[waveNames[n%len(waveNames)]])
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] += rand.Float64()
		}

		doc := NewSimpleDocument(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := waveforms["risingstep"]
	d := SimpleDocument{Vector: query}

	so := NewDefaultSearchOptions()
	so.SignFilter = SignFilter_POS

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkLSHDelete(b *testing.B) {
	opt := NewDefaultOptions()
	opt.VectorLength = 60
	lsh, err := New(opt)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, opt.VectorLength)
		for j := 0; j < opt.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := NewSimpleDocument(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := lsh.Delete(uint64(i))
		if err != nil {
			if err == ErrDocumentNotStored {
				continue
			}
			b.Fatal(err)
		}
	}
}
